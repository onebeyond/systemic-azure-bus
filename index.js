const debug = require('debug')('systemic-azure-bus');
const { join } = require('path');
const requireAll = require('require-all');
const { Namespace } = require('@azure/service-bus');
const initTopicClientFactory = require('./lib/topicClientFactory');

module.exports = () => {
	let connection;
	let topicClientFactory;

	const start = async ({ config: { connection: { connectionString }, subscriptions, publications } }) => {
		const errorStrategies = requireAll(join(__dirname, 'lib', 'errorStrategies'));

		connection = Namespace.createFromConnectionString(connectionString);
		topicClientFactory = initTopicClientFactory(connection);

		const publish = publicationId => {
			const { topic, contentType = 'application/json' } = publications[publicationId] || {};
			if (!topic) throw new Error(`Topic for publication ${publicationId} non found!`);
			const sender = topicClientFactory.createSender(topic);
			return async (body, label = '', userProperties) => {
				const message = {
					body,
					contentType,
					label,
					userProperties: {
						...userProperties,
						attemptCount: 0,
					},
				};
				await sender.send(message);
			};
		};

		const subscribe = onError => (subscriptionId, handler) => {
			const { topic, subscription, errorHandling } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const receiver = topicClientFactory.createReceiver(topic, subscription);

			const onMessageHandler = async brokeredMessage => {
				const topicErrorStrategies = {
					retry: errorStrategies.retry(topic),
					deadLetter: errorStrategies.deadLetter(topic),
					exponentialBackoff: errorStrategies.exponentialBackoff(topic, topicClientFactory),
				};

				try {
					debug(`Handling message on topic ${topic}`);
					await handler({ body: brokeredMessage.body, userProperties: brokeredMessage.userProperties });
					await brokeredMessage.complete();
				} catch (e) {
					const subscriptionErrorStrategy = (errorHandling || {}).strategy;
					const errorStrategy = e.strategy || subscriptionErrorStrategy || 'retry';
					debug(`Handling error with strategy ${errorStrategy} on topic ${topic}`);
					const errorHandler = topicErrorStrategies[errorStrategy] || topicErrorStrategies.retry;
					await errorHandler(brokeredMessage, errorHandling || {});
				}
			};
			debug(`Starting subscription ${subscriptionId} on topic ${topic}...`);
			receiver.receive(onMessageHandler, onError, { autoComplete: false });
		};

		const peekDlq = async subscriptionId => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const dlqName = Namespace.getDeadLetterTopicPath(topic, subscription);
			const client = connection.createQueueClient(dlqName);
			const peekedDeadMsgs = await client.peek();
			debug(`Peeked ${peekedDeadMsgs.length} messages from DLQ ${dlqName}`);
			await client.close();
			return peekedDeadMsgs;
		};

		const processDlq = async (subscriptionId, handler) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const dlqName = Namespace.getDeadLetterTopicPath(topic, subscription);
			const client = connection.createQueueClient(dlqName);
			const receiver = client.getReceiver();

			const deadMsgs = await peekDlq(subscriptionId);
			for (const item of deadMsgs) { // eslint-disable-line no-unused-vars,no-restricted-syntax
				const messages = await receiver.receiveBatch(1); // eslint-disable-line no-await-in-loop
				debug(`Processing message from DLQ ${dlqName}`);
				await handler(messages[0]); // eslint-disable-line no-await-in-loop
			}
			await client.close();
		};

		return {
			publish,
			subscribe,
			peekDlq,
			processDlq,
		};
	};

	const stop = async () => {
		await topicClientFactory.stop();
		debug('Stopping service bus connection...');
		await connection.close();
	};

	return { start, stop };
};
