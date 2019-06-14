const debug = require('debug')('systemic-azure-bus');
const zlib = require('zlib');
const { join } = require('path');
const requireAll = require('require-all');
const { ServiceBusClient, TopicClient } = require('@azure/service-bus');

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
	const senders = [];

	const start = async ({
		config: {
			connection: { connectionString },
			subscriptions,
			publications,
		},
	}) => {
		connection = ServiceBusClient.createFromConnectionString(connectionString);
		topicClientFactory = factories.topics(connection);
		queueClientFactory = factories.queue(connection);

		const publish = publicationId => {
			const { topic } = publications[publicationId] || {};
			if (!topic) throw new Error(`Topic for publication ${publicationId} non found!`);
			let { sender } = senders.find(s => s.publicationId === publicationId) || {};
			if (!sender) {
				sender = topicClientFactory.createSender(topic);
				senders.push({ publicationId, sender });
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

		const peekDlq = async subscriptionId => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const dlqName = TopicClient.getDeadLetterTopicPath(topic, subscription);
			const client = connection.createQueueClient(dlqName);
			const peekedMessage = await client.peek();
			debug(`Peeked message from DLQ ${dlqName}`);
			await client.close();
			return peekedMessage;
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

		return {
			publish,
			subscribe,
			peekDlq,
			processDlq,
		};
	};

	const stop = async () => {
		await topicClientFactory.stop();
		await queueClientFactory.stop();
		debug('Stopping service bus connection...');
		await connection.close();
		const checkifSubscriptionIsEmpty = () => new Promise(resolve => setInterval(() => {
			debug(`Trying to stop component | ${enqueuedItems} items remaining`);
			enqueuedItems === 0 && resolve(); // eslint-disable-line no-unused-expressions
		}, 100));
		await checkifSubscriptionIsEmpty();
	};

	return { start, stop };
};
