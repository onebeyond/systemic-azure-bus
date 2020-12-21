const debug = require('debug')('systemic-azure-bus:factory:topic');

module.exports = connection => {
	const registeredClients = [];
	const registeredReceivers = [];
	const registeredSenders = [];

	const createSender = topic => {
		debug(`Preparing connection to publish on topic ${topic}...`);

		const topicSender = connection.createSender(topic);
		registeredSenders.push(topicSender);
		return topicSender;
	};

	const createReceiver = (topic, subscription) => {
		debug(`Preparing connection to receive messages from topic ${topic} on subscription ${subscription}...`);
		const client = connection.createSubscriptionClient(topic, subscription);
		registeredClients.push(client);
		const receiver = client.createReceiver();
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
		debug('Stopping registered senders...');
		for (const sender of registeredSenders) { // eslint-disable-line no-restricted-syntax
			await sender.close(); // eslint-disable-line no-await-in-loop
		}
	};

	return {
		createSender,
		createReceiver,
		stop,
	};
};
