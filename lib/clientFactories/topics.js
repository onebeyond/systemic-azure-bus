const debug = require('debug')('systemic-azure-bus:factory:topic');

module.exports = sbClient => {
	const registeredReceivers = [];
	const registeredSenders = [];

	const createSender = topic => {
		debug(`Preparing connection to publish on topic ${topic}...`);

		const topicSender = sbClient.createSender(topic);
		registeredSenders.push(topicSender);
		return topicSender;
	};

	const createReceiver = ({ topic, subscription, mode = 'peekLock', isDlq = false }) => {
		debug(`Preparing connection to receive messages from topic ${topic} on subscription ${subscription}...`);

		const options = { mode };
		if (isDlq) options.subQueueType = 'deadLetter';

		const subscriptionReceiver = sbClient.createReceiver(topic, subscription, options);
		registeredReceivers.push(subscriptionReceiver);
		return subscriptionReceiver;
	};

	const stop = async () => {
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
