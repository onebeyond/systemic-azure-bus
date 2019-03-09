const debug = require('debug')('systemic-azure-bus:topicClientFactory');

module.exports = (connection) => {
	const registeredClients = [];
	const registeredReceivers = [];

	const createSender = (topic) => {
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
    for (const client of registeredClients) {
      await client.close();
    };
    debug('Stopping registered receivers...');
    for (const receiver of registeredReceivers) {
      await receiver.close();
    };
	};

	return {
		createSender,
		createReceiver,
		stop,
	};

};