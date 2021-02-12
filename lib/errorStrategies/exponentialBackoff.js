// https://markheath.net/post/defer-processing-azure-service-bus-message
const moment = require('moment');
const debug = require('debug')('systemic-azure-bus:errors:exponentialBackoff');
const deadLetter = require('./deadLetter');

const MAX_ATTEMPTS = 10;
const BACKOFF_FACTOR = 2;
const isAcceptable = attempts => attempts > 0 && attempts <= MAX_ATTEMPTS;
// we don't know if here we have to remove the property userProperties.attemptCount and just leave deliveryCount
const extractCurrentAttempt = brokeredMessage => brokeredMessage.applicationProperties.attemptCount || brokeredMessage.deliveryCount;

module.exports = (topic, topicClientFactory, receiver) => async (brokeredMessage, { options = { measure: 'minutes', attempts: MAX_ATTEMPTS } }) => {
	const attempt = extractCurrentAttempt(brokeredMessage);
	const attemptsLimit = (isAcceptable(options.attempts)) ? options.attempts : MAX_ATTEMPTS;
	const limitReached = (attempt + 1) === attemptsLimit;

	const copy = (message, currentAttempt) => {
		const messageCopy = { ...message };
		messageCopy.deliveryCount = currentAttempt + 1;
		return messageCopy;
	};

	const reschedule = async (message, msgAttempt) => {
		const calculateNextAttempt = messageAttempt => BACKOFF_FACTOR ** messageAttempt;
		const calculateScheduledTime = (nextAttempt, measure) => moment().add(nextAttempt, measure).toDate().getTime();
		const schedule = async (msg, scheduledTimeInMillisecs) => {
			const sender = topicClientFactory.createSender(topic);
			await sender.scheduleMessages([msg], new Date(scheduledTimeInMillisecs));
		};

		const nextAttempt = calculateNextAttempt(msgAttempt);
		const scheduledTime = calculateScheduledTime(nextAttempt, options.measure);
		debug(`Retrying message exponentially with number of attempts ${msgAttempt} on topic ${topic}. Scheduling for ${nextAttempt} ${options.measure}...`);
		const copiedMessage = copy(message, msgAttempt);
		await Promise.all([
			schedule(copiedMessage, scheduledTime),
			receiver.completeMessage(message),
		]);
	};


	if (limitReached) {
		debug(`Maximum number of deliveries (${attemptsLimit}) reached on topic ${topic}. Sending to dlq...`);
		await deadLetter(topic, receiver)(brokeredMessage);
		return Promise.resolve();
	}
	await reschedule(brokeredMessage, attempt);
	return Promise.resolve();
};
