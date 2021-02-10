const debug = require('debug')('systemic-azure-bus:factory:queue');

module.exports = connection => {
	const registeredSenders = [];
	const registeredReceivers = [];

	const createSender = queue => {
		debug(`Preparing connection to publish on queue ${queue}...`);

		const queueSender = connection.createSender(queue);
		registeredSenders.push(queueSender);
		return queueSender;
	};

	const createReceiver = ({ queue, mode = 'peekLock', isDlq }) => {
		debug(`Preparing connection to receive messages from queue ${queue}...`);
		const options = { mode };
		if (isDlq) options.subQueueType = 'deadLetter';

		const queueReceiver = connection.createReceiver(queue, options);
		registeredReceivers.push(queueReceiver);
		return queueReceiver;
	};

	const stop = async () => {
		debug('Stopping registered clients...');
		for (const sender of registeredSenders) { // eslint-disable-line no-restricted-syntax
			await sender.close(); // eslint-disable-line no-await-in-loop
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
