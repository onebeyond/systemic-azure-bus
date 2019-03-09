// https://markheath.net/post/defer-processing-azure-service-bus-message
const moment = require('moment');
const debug = require('debug')('systemic-azure-bus:errors:exponentialBackoff');
const deadLetter = require('./deadLetter');

const MAX_ATTEMPTS = 10;
const BACKOFF_FACTOR = 2;
module.exports = (topic, connection) => async (brokeredMessage, { options = { measure: 'minutes', attempts: MAX_ATTEMPTS } }) => {
  const schedule = async (message, scheduledTimeInMillisecs) => {
    const client = connection.createTopicClient(topic);
    // registeredClients.push(client);
    const sender = client.getSender();
    await sender.scheduleMessages(new Date(scheduledTimeInMillisecs), [message]);
  };

  const attemptsLimit = (options.attempts > 0 && options.attempts <= 10) ? options.attempts : MAX_ATTEMPTS;
  const { measure } = options;
  const attempt = brokeredMessage.userProperties.attemptCount || brokeredMessage.deliveryCount;
  if ((attempt + 1) === attemptsLimit) {
    debug(`Maximum number of deliveries (${attemptsLimit}) reached on topic ${topic}. Sending to dlq...`);
    await deadLetter(topic)(brokeredMessage);
  } else {
    const nextAttempt = Math.pow(BACKOFF_FACTOR, attempt);
    const scheduledTime = moment().add(nextAttempt, measure).toDate().getTime();
    debug(`Retrying message exponentially with number of attempts ${attempt} on topic ${topic}. Scheduling for ${nextAttempt} ${measure}...`);
    const clone = brokeredMessage.clone();
    clone.userProperties = {
      attemptCount: attempt + 1,
    };
    await Promise.all([
      schedule(clone, scheduledTime),
      brokeredMessage.complete(),
    ]);
  }
};