/* eslint-disable no-await-in-loop */
const debug = require('debug')('systemic-azure-bus');
const zlib = require('zlib');
const { join } = require('path');
const requireAll = require('require-all');
const { ServiceBusClient } = require('@azure/service-bus');

const errorStrategies = requireAll(join(__dirname, 'lib', 'errorStrategies'));
const factories = requireAll(join(__dirname, 'lib', 'clientFactories'));
const topicApi = requireAll(join(__dirname, 'lib', 'operations', 'topics'));

const decodingStrategies = {
	zlib: body => JSON.parse(zlib.inflateSync(body)),
	default: body => body,
};

const getBodyDecoded = (body, contentEncoding) => (decodingStrategies[contentEncoding] || decodingStrategies.default)(body);

module.exports = () => {
	let connection;
	let topicClientFactory;
	let queueClientFactory;
	let enqueuedItems = 0;
	const sendersByPublication = [];

	const start = async ({
		config: {
			connection: { connectionString },
			subscriptions,
			publications,
		},
	}) => {
		connection = new ServiceBusClient(connectionString);
		topicClientFactory = factories.topics(connection);
		queueClientFactory = factories.queue(connection);

		const publish = publicationId => {
			const { topic } = publications[publicationId] || {};
			if (!topic) throw new Error(`Topic for publication ${publicationId} non found!`);
			let { sender } = sendersByPublication.find(senderByPub => senderByPub.publicationId === publicationId) || {};
			if (!sender) {
				sender = topicClientFactory.createSender(topic);
				sendersByPublication.push({ publicationId, sender });
			}
			return topicApi.publish(sender);
		};

		const getProperties = message => ({
			entity: message._context.entityPath,
			messageId: message.messageId,
			contentType: message._amqpMessage.content_type,
		});


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
					enqueuedItems++;
					debug(`Enqueued items increase | ${enqueuedItems} items`);
					debug(`Handling message on topic ${topic}`);
					await handler({ body: getBodyDecoded(brokeredMessage.body, brokeredMessage.userProperties.contentEncoding), userProperties: brokeredMessage.userProperties, properties: getProperties(brokeredMessage) });
					await brokeredMessage.complete();
				} catch (e) {
					const subscriptionErrorStrategy = (errorHandling || {}).strategy;
					const errorStrategy = e.strategy || subscriptionErrorStrategy || 'retry';
					debug(`Handling error with strategy ${errorStrategy} on topic ${topic}`);
					const errorHandler = topicErrorStrategies[errorStrategy] || topicErrorStrategies.retry;
					await errorHandler(brokeredMessage, errorHandling || {});
				} finally {
					enqueuedItems--;
					debug(`Enqueued items decrease | ${enqueuedItems} items`);
				}
			};


			debug(`Starting subscription ${subscriptionId} on topic ${topic}...`);
			// receiver.registerMessageHandler(onMessageHandler, onError, { autoComplete: false });
			receiver.subscribe({
				processMessage: onMessageHandler,
				processError: onError,
			}, { autoCompleteMessages: false });
		};

		const peekDlq = async (subscriptionId, messagesNumber = 1) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);

			const deletedQueueReceiver = connection.createReceiver(topic, subscription);

			const peekedMessages = await deletedQueueReceiver.receiveMessages(messagesNumber, { maxWaitTimeInMs: 10000 });
			debug(`${peekedMessages.length} peeked messages from DLQ ${deletedQueueReceiver}`);
			await deletedQueueReceiver.close();
			return peekedMessages;
		};

		const peek = async (subscriptionId, messagesNumber = 1) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const queueReceiver = connection.createReceiver(`${topic}/Subscriptions/${subscription}`);
			const activeMessages = await queueReceiver.peekMessages(messagesNumber);
			debug(`${activeMessages.length} peeked messages from Active Queue`);
			await queueReceiver.close();
			return activeMessages;
		};

		const processDlq = async (subscriptionId, handler) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);

			const deletedQueueReceiver = connection.createReceiver(topic, subscription);

			while ((messages = await deletedQueueReceiver.receiveMessages(1, { maxWaitTimeInMs: 10000 })) && messages.length > 0) { // eslint-disable-line no-undef, no-cond-assign, no-await-in-loop
				debug('Processing message from DLQ');
				await handler(messages[0]); // eslint-disable-line no-undef, no-await-in-loop
			}
			await deletedQueueReceiver.close();
		};

		const emptyDlq = async subscriptionId => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);

			try {
				const deletedQueueReceiver = connection.createReceiver(topic, { receiveMode: 'receiveAndDelete' });

				let messagesPending = true;
				const getMessagesFromDlq = async () => {
					const messages = await deletedQueueReceiver.receiveMessages(50, { maxWaitTimeInMs: 10000 });
					if (messages.length === 0) {
						debug('There are no messages in this Dead Letter Queue');
						messagesPending = false;
					} else if (messages.length < 50) {
						debug(`processing last ${messages.length} messages from DLQ`);
						messagesPending = false;
					} else {
						debug('processing last 50 messages from DLQ');
					}
				};
				while (messagesPending) {
					await getMessagesFromDlq();
				}
				await deletedQueueReceiver.close();
			} catch (err) {
				debug(`Error while deleting dead letter queue: ${err.message}`);
				throw (err);
			}
		};

		const getSubscriptionRules = async subscriptionId => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const client = connection.createSubscriptionClient(topic, subscription);
			const rules = await client.getRules();
			return rules;
		};

		const health = async () => {
			const subscriptionNames = Object.keys(subscriptions);
			const getConfigTopic = name => subscriptions[name].topic;
			const getConfigSubscription = name => subscriptions[name].subscription;
			const createClient = name => connection.createSubscriptionClient(getConfigTopic(name), getConfigSubscription(name));
			let healthCheck;
			try {
				const clients = subscriptionNames.map(createClient);
				const healthchecks = clients.map(c => c.peek());
				await Promise.all(healthchecks);
				await Promise.all(clients.map(c => c.close()));
				healthCheck = {
					status: 'ok',
				};
			} catch (err) {
				healthCheck = {
					status: 'error',
					details: err.message,
				};
			}
			return healthCheck;
		};

		return {
			health,
			publish,
			subscribe,
			peekDlq,
			peek,
			processDlq,
			emptyDlq,
			getSubscriptionRules,
		};
	};

	const stop = async () => {
		await topicClientFactory.stop();
		await queueClientFactory.stop();
		debug('Stopping service bus connection...');
		await connection.close();
		const checkifSubscriptionIsEmpty = () => new Promise(resolve => setInterval(() => {
			debug(`Trying to stop component | ${enqueuedItems} enqueued items remaining`);
			enqueuedItems === 0 && resolve(); // eslint-disable-line no-unused-expressions
		}, 100));
		await checkifSubscriptionIsEmpty();
		sendersByPublication.length = 0;
	};

	return { start, stop };
};
