const debug = require('debug')('systemic-azure-bus:errors:retry');

module.exports = (topic, receiver) => async brokeredMessage => {
	debug(`Retrying message with number of attempts ${brokeredMessage.applicationProperties.attempCount + 1} on topic ${topic}`);
	await receiver.abandonMessage(brokeredMessage);
};
