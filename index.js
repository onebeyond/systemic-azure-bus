// https://github.com/Azure/azure-sdk-for-js/blob/master/packages/%40azure/servicebus/data-plane/test/receiveAndDeleteMode.spec.ts#L416-L424

const debug = require('debug')('systemic-azure-bus');
const { Namespace, delay } = require('@azure/service-bus');

module.exports = () => {
	const registeredClients = [];
	const registeredReceivers = [];
	let connection;

	const start = async ({ config: { connection: { connectionString }, subscriptions, publications } }) => {

		connection = Namespace.createFromConnectionString(connectionString);

		const publish = publicationId => {
			const { topic } = publications[publicationId] || {};
			if (!topic) throw new Error(`Topic for publication ${publicationId} non found!`);
			debug(`Preparing connection to publish ${publicationId} on topic ${topic}...`);
			const client = connection.createTopicClient(topic);
			registeredClients.push(client);
			const sender = client.getSender();
			return async (body, label = '') => {
				const message = {
					body,
					contentType: 'application/json',
					label,
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
				const { body, deliveryCount } = brokeredMessage;

				const errorStrategies = {
					retry: async () => {
						debug(`Abandoning message with number of attempts ${deliveryCount} on topic ${topic}`);
						/* Everytime you abandon a message, delivery count will be increased by 1.
						When it reaches to max delivery count (which is 10 default), it will be sent to dead queue */
						await brokeredMessage.abandon();
					},
					dlq: async () => {
						debug(`Sending message straight to DLQ on topic ${topic}`);
						await brokeredMessage.deadLetter();
					}
				};

				try {
					debug(`Handling message on topic ${topic}`);
					await handler(body);
					await brokeredMessage.complete();
				} catch(e) {
					const subscriptionErrorStrategy = (errorHandling || {}).strategy;
					const errorStrategy = e.strategy || subscriptionErrorStrategy || 'retry';
					debug(`Handling error with strategy ${errorStrategy} on topic ${topic}`);
					const errorHandler = errorStrategies[errorStrategy] || errorStrategies['retry'];
					await errorHandler();
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


			while (true) {
				const messages = await receiver.receiveBatch(1);
				if (!messages.length) {
					debug(`No more messages to receive from DLQ ${dlqName}`);
					break;
				}
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