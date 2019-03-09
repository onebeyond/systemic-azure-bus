const debug = require('debug')('systemic-azure-bus:errors:retry');

module.exports = (topic) => async (brokeredMessage) => {
  debug(`Retrying message with number of attempts ${brokeredMessage.deliveryCount + 1} on topic ${topic}`);
  await brokeredMessage.abandon();
};