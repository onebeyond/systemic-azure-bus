/* eslint-disable no-await-in-loop */
const debug = require('debug')('systemic-azure-bus');
const zlib = require('zlib');
const { join } = require('path');
const requireAll = require('require-all');
const { ServiceBusClient, ServiceBusAdministrationClient } = require('@azure/service-bus');

const errorStrategies = requireAll(join(__dirname, 'lib', 'errorStrategies'));
const factories = requireAll(join(__dirname, 'lib', 'clientFactories'));
const topicApi = requireAll(join(__dirname, 'lib', 'operations', 'topics'));

const decodingStrategies = {
	zlib: body => JSON.parse(zlib.inflateSync(body)),
	default: body => body,
};

const getBodyDecoded = (body, contentEncoding) => (decodingStrategies[contentEncoding] || decodingStrategies.default)(body);

module.exports = () => {
	let sbClient;
	let topicClientFactory;
	let queueClientFactory;
	let enqueuedItems = 0;
	let sendersByPublication = {};
	const openSubscriptions = [];

	const start = async ({
		config: {
			connection: { connectionString },
			subscriptions,
			publications,
		},
	}) => {
		sbClient = new ServiceBusClient(connectionString);
		topicClientFactory = factories.topics(sbClient);
		queueClientFactory = factories.queue(sbClient);

		const publish = publicationId => {
			const { topic } = publications[publicationId] || {};
			if (!topic) throw new Error(`Topic for publication ${publicationId} non found!`);
			let sender = sendersByPublication[publicationId];
			if (!sender) {
				sender = topicClientFactory.createSender(topic);
				sendersByPublication[publicationId] = sender;
			}
			return topicApi.publish(sender);
		};

		const getMessageProperties = brokeredMessage => {
			const {
				// ServiceBusReceivedMessage
				deadLetterReason,
				deadLetterErrorDescription,
				lockToken,
				deliveryCount,
				enqueuedTimeUtc,
				expiresAtUtc,
				lockedUntilUtc,
				enqueuedSequenceNumber,
				sequenceNumber,
				deadLetterSource,
				state,

				// ServiceBusMessage
				messageId,
				contentType,
				correlationId,
				partitionKey,
				sessionId,
				replyToSessionId,
				timeToLive,
				subject,
				to,
				replyTo,
				scheduledEnqueueTimeUtc,
			} = brokeredMessage;

			return {
				// ServiceBusReceivedMessage
				deadLetterReason,
				deadLetterErrorDescription,
				lockToken,
				deliveryCount,
				enqueuedTimeUtc,
				expiresAtUtc,
				lockedUntilUtc,
				enqueuedSequenceNumber,
				sequenceNumber,
				deadLetterSource,
				state,

				// ServiceBusMessage
				messageId,
				contentType,
				correlationId,
				partitionKey,
				sessionId,
				replyToSessionId,
				timeToLive,
				subject,
				to,
				replyTo,
				scheduledEnqueueTimeUtc,
			};
		};

		const subscribe = onError => (subscriptionId, handler) => {
			const { topic, subscription, errorHandling } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const receiver = topicClientFactory.createReceiver({ topic, subscription });
			const topicErrorStrategies = {
				retry: errorStrategies.retry(topic, receiver),
				deadLetter: errorStrategies.deadLetter(topic, receiver),
				exponentialBackoff: errorStrategies.exponentialBackoff(topic, topicClientFactory, receiver),
			};

			const onMessageHandler = async brokeredMessage => {
				try {
					enqueuedItems++;
					debug(`Enqueued items increase | ${enqueuedItems} items`);
					debug(`Handling message on topic ${topic}`);
					const { body, applicationProperties } = brokeredMessage;
					const { subscriptionName: messageSubscription } = applicationProperties;

					if (!messageSubscription || subscription === messageSubscription) {
						/**
						 * The handler is only going to run if the "messageSubscription" property
						 * does not exists. Or if it exists and is the current subscription from all
						 * the different ones that the topic can contain.
						 * But the message confirmation operation will always be done, even if the handler
						 * is not executed because of the comment above.
						 */
						await handler({
							body: getBodyDecoded(
								body,
								applicationProperties.contentEncoding,
							),
							applicationProperties,
							properties: getMessageProperties(brokeredMessage),
						});
					}
					await receiver.completeMessage(brokeredMessage);
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
			const openSubscription = receiver.subscribe({
				processMessage: onMessageHandler,
				processError: async args => {
					onError(args.error);
				},
			}, { autoCompleteMessages: false });
			openSubscriptions.push(openSubscription);
		};

		const peekDlq = async (subscriptionId, messagesNumber = 1) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			// check access to dlq by topic and client
			const dlqReceiver = topicClientFactory.createReceiver({ topic, subscription, isDlq: true });

			const peekedMessages = await dlqReceiver.receiveMessages(messagesNumber, { maxWaitTimeInMs: 3000 });
			debug(`${peekedMessages.length} peeked messages from DLQ ${dlqReceiver.entityPath}`);
			await dlqReceiver.close();
			return peekedMessages;
		};

		const peek = async (subscriptionId, messagesNumber = 1) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const topicReceiver = topicClientFactory.createReceiver({ topic, subscription });
			const activeMessages = await topicReceiver.peekMessages(messagesNumber);
			debug(`${activeMessages.length} peeked messages from Active Queue`);
			await topicReceiver.close();
			return activeMessages;
		};

		const processDlq = async (subscriptionId, handler) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);

			const dlqReceiver = topicClientFactory.createReceiver({ topic, subscription, isDlq: true });

			while ((messages = await dlqReceiver.receiveMessages(1, { maxWaitTimeInMs: 3000 })) && messages.length > 0) { // eslint-disable-line no-undef, no-cond-assign, no-await-in-loop
				debug('Processing message from DLQ');
				await handler(messages[0], dlqReceiver); // eslint-disable-line no-undef, no-await-in-loop
			}
			await dlqReceiver.close();
		};

		const emptyDlq = async subscriptionId => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);

			try {
				const dlqReceiver = topicClientFactory.createReceiver({ topic, subscription, mode: 'receiveAndDelete', isDlq: true });

				let messagesPending = true;
				const getMessagesFromDlq = async () => {
					const messages = await dlqReceiver.receiveMessages(50, { maxWaitTimeInMs: 3000 });
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
				await dlqReceiver.close();
			} catch (err) {
				debug(`Error while deleting dead letter queue: ${err.message}`);
				throw (err);
			}
		};

		const getSubscriptionRules = async subscriptionId => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const adminClient = new ServiceBusAdministrationClient(connectionString);
			const rules = await adminClient.getRules();
			return rules;
		};

		const health = async () => {
			const subscriptionNames = Object.keys(subscriptions);
			const getConfigTopic = name => subscriptions[name].topic;
			const getConfigSubscription = name => subscriptions[name].subscription;
			const createClient = name => topicClientFactory.createReceiver({ topic: getConfigTopic(name), subscription: getConfigSubscription(name) });
			let healthCheck;
			try {
				const clients = subscriptionNames.map(createClient);
				const healthchecks = clients.map(c => c.peekMessages(1));
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

	const drainOpenSubscriptions = async () => {
		debug(`Closing ${openSubscriptions.length} active subscriptions`);
		const closeSubscription = async (subscription, index) => {
			debug('Closing active subscription', index);
			await subscription.close();
		};
		await Promise.all(openSubscriptions.map(closeSubscription));
	};

	const stop = async () => {
		debug('Closing open stream subscriptions and draining...');
		await drainOpenSubscriptions();

		let timer;
		// eslint-disable-next-line no-return-assign
		const checkIfSubscriptionIsEmpty = () => new Promise(resolve => {
			timer = setInterval(async () => {
				debug(`Trying to stop component | ${enqueuedItems} enqueued items remaining`);
				// eslint-disable-next-line no-unused-expressions
				enqueuedItems === 0 && resolve();
			}, 100);
		});
		await checkIfSubscriptionIsEmpty();
		clearInterval(timer);

		sendersByPublication = {};

		debug('Closing topic client factory...');
		await topicClientFactory.stop();
		debug('Closing queue client factory...');
		await queueClientFactory.stop();
		debug('Closing ServiceBusClient connection...');
		await sbClient.close();
	};

	return { start, stop };
};
