/* eslint-disable no-await-in-loop */
const debug = require('debug')('systemic-azure-bus');
const zlib = require('zlib');
const { join } = require('path');
const requireAll = require('require-all');
const { ServiceBusClient, TopicClient, ReceiveMode } = require('@azure/service-bus');

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
			receiver.registerMessageHandler(onMessageHandler, onError, { autoComplete: false });
		};

		const peekDlq = async (subscriptionId, n) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const dlqName = TopicClient.getDeadLetterTopicPath(topic, subscription);
			const client = connection.createQueueClient(dlqName);
			const peekedMessages = await client.peek(n);
			debug(`${peekedMessages.length} peeked messages from DLQ ${dlqName}`);
			await client.close();
			return peekedMessages;
		};

		const peek = async (subscriptionId, n = 1) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const queueSender = connection.createSender(`${topic}/Subscriptions/${subscription}`);
			const activeMessages = await queueSender.peekMessages(n);
			debug(`${activeMessages.length} peeked messages from Active Queue`);
			await queueSender.close();
			return activeMessages;
		};

		const processDlq = async (subscriptionId, handler) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const dlqName = TopicClient.getDeadLetterTopicPath(topic, subscription);
			const receiver = queueClientFactory.createReceiver(dlqName);
			while ((messages = await receiver.receiveMessages(1, 5)) && messages.length > 0) { // eslint-disable-line no-undef, no-cond-assign, no-await-in-loop
				debug('Processing message from DLQ');
				await handler(messages[0]); // eslint-disable-line no-undef, no-await-in-loop
			}
			receiver.close();
		};

		const emptyDlq = async subscriptionId => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const dlqName = TopicClient.getDeadLetterTopicPath(topic, subscription);
			try {
				const receiver = queueClientFactory.createReceiver(dlqName, ReceiveMode.receiveAndDelete);
				let messagesPending = true;
				const getMessagesFromDlq = async () => {
					const messages = await receiver.receiveMessages(50, 10);
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
				await receiver.close();
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
