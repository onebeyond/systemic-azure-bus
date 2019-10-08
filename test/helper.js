const debug = require('debug')('systemic-azure-bus:test:helper');

const initBus = require('..');

const busApi = initBus();

const enoughTime = 500;
const schedule = fn => setTimeout(fn, enoughTime);
const createPayload = () => ({ foo: Date.now() });

let bus;

const purgeDlqBySubcriptionId = async subscriptionId => {
	const accept = async message => {
		await message.complete();
		return Promise.resolve();
	};
	const deadBodies = await bus.peekDlq(subscriptionId);
	debug(`Peeked ${deadBodies.length} messages in DLQ of ${subscriptionId}`);
	if (deadBodies.length === 0) return;
	await bus.processDlq(subscriptionId, accept);
};

const purgeActiveBySubcriptionId = async (subscriptionId, n) => {
	let activeBodies;
	try {
		activeBodies = await bus.peekActive(subscriptionId, n);
	} catch (error) {
		console.log(error);
	}
	const processActiveMessages = async () => new Promise((resolve, reject) => {
		const timeout = setTimeout(() => {
			reject();
		}, 5000);

		let count = 0;
		const processMessage = () => {
			count++;
			if (count === activeBodies.length) {
				clearTimeout(timeout);
				resolve();
			}
		};
		const subscribe = () => bus.subscribe(console.error, console.log);
		subscribe()(subscriptionId, processMessage);
	});
	debug(`Peeked ${activeBodies.length} messages in DLQ of ${subscriptionId}`);
	if (activeBodies.length === 0) return;
	await processActiveMessages();
};

const start = async ({ config }) => {
	debug('Initialising service bus API...');
	bus = await busApi.start({ config });
	return {
		safeSubscribe: bus.subscribe(console.error, console.log), // eslint-disable-line no-console
		publish: bus.publish,
		peekDlq: bus.peekDlq,
		peekActive: bus.peekActive,
		purgeDlqBySubcriptionId,
		purgeActiveBySubcriptionId,
		processDlq: bus.processDlq,
		health: bus.health,
	};
};

const stop = async () => {
	debug('Stopping service bus API...');
	await busApi.stop();
};

module.exports = {
	createPayload,
	schedule,
	bus: { start, stop },
};
