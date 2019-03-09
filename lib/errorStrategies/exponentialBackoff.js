const MAX_ATTEMPTS = 10;
const BACKOFF_FACTOR = 2;
module.exports = (topic) => async (brokeredMessage, { options = { measure: 'minutes', attempts: MAX_ATTEMPTS } }) => {
  // https://markheath.net/post/defer-processing-azure-service-bus-message
  const attemptsLimit = (options.attempts > 0 && options.attempts <= 10) ? options.attempts : MAX_ATTEMPTS;
  const { measure } = options;
  const attempt = userProperties.attemptCount || deliveryCount;
  if ((attempt + 1) === attemptsLimit) {
    debug(`Maximum number of deliveries (${attemptsLimit}) reached on topic ${topic}. Sending to dlq...`);
    await deadLetter();
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