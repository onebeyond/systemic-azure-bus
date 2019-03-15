const debug = require('debug')('systemic-azure-bus:topicClientFactory');

module.exports = connection => {
	const registeredClients = [];
	const registeredReceivers = [];

	const createSender = topic => {
		debug(`Preparing connection to publish on topic ${topic}...`);
		const client = connection.createTopicClient(topic);
		registeredClients.push(client);
		return client.getSender();
	};

	const createReceiver = (topic, subscription) => {
		debug(`Preparing connection to receive messages from topic ${topic} on subscription ${subscription}...`);
		const client = connection.createSubscriptionClient(topic, subscription);
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
