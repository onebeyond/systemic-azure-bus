const debug = require('debug')('systemic-azure-bus:factory:queue');
const { ReceiveMode } = require('@azure/service-bus');

module.exports = connection => {
	const registeredClients = [];
	const registeredReceivers = [];

	const createSender = queue => {
		debug(`Preparing connection to publish on queue ${queue}...`);

		const queueSender = connection.createSender(queue);
		registeredClients.push(queueSender);
		return queueSender;
	};

	const createReceiver = (queue, mode = ReceiveMode.peekLock) => {
		debug(`Preparing connection to receive messages from queue ${queue}...`);

		const client = connection.createQueueClient(queue);
		registeredClients.push(client);
		const receiver = client.createReceiver(mode);
		registeredReceivers.push(receiver);
		return receiver;

		const queueReceiver = connection.createReceiver(queue);
		registeredReceivers.push(queueReceiver);
		return queueReceiver;
	};

	const stop = async () => {
		debug('Stopping registered clients...');
		for (const client of registeredClients) { // eslint-disable-line no-restricted-syntax
			await client.close(); // eslint-disable-line no-await-in-loop
		}
		debug('Stopping registered receivers...');
		for (const receiver of registeredReceivers) { // eslint-disable-line no-restricted-syntax
			await receiver.close(); // eslint-disable-line no-await-in-loop
		}
	};

	return {
		createSender,
		createReceiver,
		stop,
	};
};
