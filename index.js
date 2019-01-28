const debug = require('debug')('systemic-azure-bus');
const { creatSubscriptionListener, createTopicSender } = require('@liber/ez-pubsub');

module.exports = () => {
	const registeredSubscriptions = [];

	const start = async ({ config: { subscriptions, publications }, logger }) => {

		const publish = publicationId => {
			const { topic } = publications[publicationId] || {};
			if (!topic) throw new Error(`Topic for publication ${publicationId} non found!`);
			debug(`Preparing connection to publish ${publicationId} on topic ${topic}...`);
			const sender = createTopicSender({ topic });
			return message => sender.send({ message: { body: JSON.stringify(message) } });
		};

		const subscribe = (onError, onStop) => (subscriptionId, handler) => {
			const { topic, subscription } = subscriptions[subscriptionId] || {};
			if (!topic || !subscription) throw new Error(`Data for subscription ${subscriptionId} non found!`);
			const newSubscription = creatSubscriptionListener({
				topic,
				subscription,
				onMessage: handler,
				defaultAck: false,
				onError: error => {
					logger.error(error.message);
					onError(error);
				},
				onStop,
			});
			registeredSubscriptions.push(newSubscription);
			debug(`Starting subscription ${subscriptionId} on topic ${topic}...`);
			newSubscription.start();
		};

		return {
			publish,
			subscribe,
		};
	};

	const stop = async () => registeredSubscriptions.forEach(subscription => subscription.stop());

	return { start, stop };
};