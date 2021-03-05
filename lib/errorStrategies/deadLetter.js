const debug = require('debug')('systemic-azure-bus:errors:deadLetter');

module.exports = (topic, receiver) => async brokeredMessage => {
	debug(`Sending message straight to DLQ on topic ${topic}`);
	await receiver.deadLetterMessage(brokeredMessage);
};
