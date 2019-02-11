// https://github.com/Azure/azure-sdk-for-js/blob/master/packages/%40azure/servicebus/data-plane/test/receiveAndDeleteMode.spec.ts#L416-L424

const debug = require('debug')('systemic-azure-bus');
const { Namespace, delay } = require('@azure/service-bus');

module.exports = () => {
	const registeredClients = [];
	const registeredReceivers = [];
	let connection;

	const start = async ({ config: { subscriptions, publications } }) => {

		connection = Namespace.createFromConnectionString(process.env.AZURE_SERVICEBUS_CONNECTION_STRING);

		const publish = publicationId => {
			const { topic } = publications[publicationId] || {};
			if (!topic) throw new Error(`Topic for publication ${publicationId} non found!`);
			debug(`Preparing connection to publish ${publicationId} on topic ${topic}...`);
			const client = connection.createTopicClient(topic);
			registeredClients.push(client);
			const sender = client.getSender();
			return async (message) => {
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
						debug(`Abandoning message with number of attempts ${deliveryCount}`);
						/* Everytime you abandon a message, delivery count will be increased by 1.
						When it reaches to max delivery count (which is 10 default), it will be sent to dead queue */
						await brokeredMessage.abandon();
					},
					dlq: async () => {
						debug('Sending message straight to DLQ');
						await brokeredMessage.deadLetter();
					}
				};

				try {
					await handler(body);
					await brokeredMessage.complete();
				} catch(e) {
					const subscriptionErrorStrategy = (errorHandling || {}).strategy;
					const errorStrategy = e.strategy || subscriptionErrorStrategy || 'retry';
					debug(`Handling error with strategy ${errorStrategy}`);
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
					console.log("No more messages to receive");
					break;
				}
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