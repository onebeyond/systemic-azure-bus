const debug = require('debug')('systemic-azure-bus:errors:retry');

module.exports = topic => async brokeredMessages => {
	debug(`Retrying message with number of attempts ${brokeredMessages.deliveryCount + 1} on topic ${topic}`);
	await brokeredMessages.abandonMessage();
};
