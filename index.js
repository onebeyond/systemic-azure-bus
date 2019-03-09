const moment = require('moment');
const debug = require('debug')('systemic-azure-bus');
const { Namespace, delay } = require('@azure/service-bus');

module.exports = () => {
	const registeredClients = [];
	const registeredReceivers = [];
	let connection;

	const start = async ({ config: { connection: { connectionString }, subscriptions, publications } }) => {

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
				const { body, deliveryCount, userProperties } = brokeredMessage;

				const deadLetter = async () => {
					debug(`Sending message straight to DLQ on topic ${topic}`);
					await brokeredMessage.deadLetter();
				};

				const retry = async () => {
					debug(`Retrying message with number of attempts ${deliveryCount + 1} on topic ${topic}`);
					await brokeredMessage.abandon();
				};

				const schedule = async (message, scheduledTimeInMillisecs) => {
					const client = connection.createTopicClient(topic);
					registeredClients.push(client);
					const sender = client.getSender();
  				await sender.scheduleMessages(new Date(scheduledTimeInMillisecs), [message]);
				};

				const MAX_ATTEMPTS = 10;
				const BACKOFF_FACTOR = 2;
				const exponentialBackoff = async ({ options = { measure: 'minutes', attempts: MAX_ATTEMPTS } }) => {
					// https://markheath.net/post/defer-processing-azure-service-bus-message
					const attemptsLimit = (options.attempts > 0 && options.attempts <= 10) ? options.attempts : MAX_ATTEMPTS;
					const { measure } = options;
					const attempt = userProperties.attemptCount || deliveryCount;
					if ((attempt + 1) === attemptsLimit) {
						debug(`Maximum number of deliveries (${attemptsLimit}) reached on topic ${topic}. Sending to dlq...`);
						await deadLetter();
					} else {
						const nextAttempt = Math.pow(BACKOFF_FACTOR, attempt);
						const scheduledTime = moment().add(nextAttempt, measure).toDate().getTime();
						debug(`Retrying message exponentially with number of attempts ${attempt} on topic ${topic}. Scheduling for ${nextAttempt} ${measure}...`);
						const clone = brokeredMessage.clone();
						clone.userProperties = {
							attemptCount: attempt + 1,
						};
						await Promise.all([
							schedule(clone, scheduledTime),
							brokeredMessage.complete(),
						]);
					}
				};

				const errorStrategies = {
					retry,
					deadLetter,
					exponentialBackoff,
				};

				try {
					debug(`Handling message on topic ${topic}`);
					await handler({ body, userProperties });
					await brokeredMessage.complete();
				} catch(e) {
					const subscriptionErrorStrategy = (errorHandling || {}).strategy;
					const errorStrategy = e.strategy || subscriptionErrorStrategy || 'retry';
					debug(`Handling error with strategy ${errorStrategy} on topic ${topic}`);
					const errorHandler = errorStrategies[errorStrategy] || errorStrategies['retry'];
					await errorHandler(errorHandling || {});
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