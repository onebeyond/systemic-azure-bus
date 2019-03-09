// https://markheath.net/post/defer-processing-azure-service-bus-message
const moment = require('moment');
const debug = require('debug')('systemic-azure-bus:errors:exponentialBackoff');
const deadLetter = require('./deadLetter');

const MAX_ATTEMPTS = 10;
const BACKOFF_FACTOR = 2;
const isAcceptable = (attempts) => attempts > 0 && attempts <= MAX_ATTEMPTS;
const extractCurrentAttempt = (brokeredMessage) => brokeredMessage.userProperties.attemptCount || brokeredMessage.deliveryCount;

module.exports = (topic, connection) => async (brokeredMessage, { options = { measure: 'minutes', attempts: MAX_ATTEMPTS } }) => {

  const attempt = extractCurrentAttempt(brokeredMessage);
  const attemptsLimit = (isAcceptable(options.attempts)) ? options.attempts : MAX_ATTEMPTS;
  const limitReached = (attempt + 1) === attemptsLimit;

  const clone = (message, currentAttempt) => {
    const clone = message.clone();
    clone.userProperties = {
      attemptCount: currentAttempt + 1,
    };
    return clone;
  };

  const reschedule = async (message, currentAttempt) => {
    const calculateNextAttempt = (currentAttempt) => Math.pow(BACKOFF_FACTOR, currentAttempt);
    const calculateScheduledTime = (nextAttempt, measure) => moment().add(nextAttempt, measure).toDate().getTime();
    const schedule = async (message, scheduledTimeInMillisecs) => {
      const client = connection.createTopicClient(topic);
      // registeredClients.push(client);
      const sender = client.getSender();
      await sender.scheduleMessages(new Date(scheduledTimeInMillisecs), [message]);
    };

    const nextAttempt = calculateNextAttempt(currentAttempt);
    const scheduledTime = calculateScheduledTime(nextAttempt, options.measure);
    debug(`Retrying message exponentially with number of attempts ${currentAttempt} on topic ${topic}. Scheduling for ${nextAttempt} ${options.measure}...`);
    const clonedMessage = clone(message, currentAttempt);
    await Promise.all([
      schedule(clonedMessage, scheduledTime),
      message.complete(),
    ]);
  };


  if (limitReached) {
    debug(`Maximum number of deliveries (${attemptsLimit}) reached on topic ${topic}. Sending to dlq...`);
    await deadLetter(topic)(brokeredMessage);
    return Promise.resolve();
  }
  await reschedule(brokeredMessage, attempt);
};