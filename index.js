const debug = require('debug')('systemic-azure-bus');
const { join } = require('path');
const requireAll = require('require-all');
const { Namespace, delay } = require('@azure/service-bus');

module.exports = () => {
	const registeredClients = [];
	const registeredReceivers = [];
	let connection;

	const start = async ({ config: { connection: { connectionString }, subscriptions, publications } }) => {

		const newErrorStrategies = requireAll(join(__dirname, 'lib', 'errorStrategies'));

		connection = Namespace.createFromConnectionString(connectionString);

		const publish = publicationId => {
			const { topic, contentType = 'application/json' } = publications[publicationId] || {};
			if (!topic) throw new Error(`Topic for publication ${publicationId} non found!`);
			debug(`Preparing connection to publish ${publicationId} on topic ${topic}...`);
			const client = connection.createTopicClient(topic);
			registeredClients.push(client);
			const sender = client.getSender();
			return async (body, label = '', userProperties) => {
				const message = {
					body,
					contentType,
					label,
					userProperties: {
						...userProperties,
						attemptCount: 0
					},
				};
				await sender.send(message);
			};
		};

		const subscribe = (onError) => (subscriptionId, handler) => {
			const { topic, subscription, errorHandling } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const client = connection.createSubscriptionClient(topic, subscription);
			registeredClients.push(client);
			const receiver = client.getReceiver();
			registeredReceivers.push(receiver);

			const onMessageHandler = async (brokeredMessage) => {

				const errorStrategies = {
					retry: newErrorStrategies.retry(topic),
					deadLetter: newErrorStrategies.deadLetter(topic),
					exponentialBackoff: newErrorStrategies.exponentialBackoff(topic, connection),
				};

				try {
					debug(`Handling message on topic ${topic}`);
					await handler({ body: brokeredMessage.body, userProperties: brokeredMessage.userProperties });
					await brokeredMessage.complete();
				} catch(e) {
					const subscriptionErrorStrategy = (errorHandling || {}).strategy;
					const errorStrategy = e.strategy || subscriptionErrorStrategy || 'retry';
					debug(`Handling error with strategy ${errorStrategy} on topic ${topic}`);
					const errorHandler = errorStrategies[errorStrategy] || errorStrategies['retry'];
					await errorHandler(brokeredMessage, errorHandling || {});
				}
			};
			debug(`Starting subscription ${subscriptionId} on topic ${topic}...`);
			receiver.receive(onMessageHandler, onError, { autoComplete: false });
		};

		const peekDlq = async (subscriptionId) => {
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
			for (const item of deadMsgs) {
				const messages = await receiver.receiveBatch(1);
				debug(`Processing message from DLQ ${dlqName}`);
				await handler(messages[0]);
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
		// await delay(5000);

		debug('Stopping registered clients...');
		registeredClients.forEach(async (client) => {
			await client.close();
		});

		// debug('Stopping registered receivers...');
		// registeredReceivers.forEach(async (receiver) => {
		// 	await receiver.close();
		// });

		// debug('Stopping service bus connection...');
		// await connection.close();
	};

	return { start, stop };
};