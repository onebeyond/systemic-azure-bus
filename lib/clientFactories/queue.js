const debug = require('debug')('systemic-azure-bus:queueClientFactory');

module.exports = connection => {
	const registeredClients = [];
	const registeredReceivers = [];

	const createSender = queue => {
		debug(`Preparing connection to publish on queue ${queue}...`);
		const client = connection.createQueueClient(queue);
		registeredClients.push(client);
		return client.getSender();
	};

	const createReceiver = queue => {
		debug(`Preparing connection to receive messages from queue ${queue}...`);
		const client = connection.createQueueClient(queue);
		registeredClients.push(client);
		const receiver = client.getReceiver();
		registeredReceivers.push(receiver);
		return receiver;
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
