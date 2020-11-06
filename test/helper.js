const debug = require('debug')('systemic-azure-bus:test:helper');

const initBus = require('..');

const busApi = initBus();

const enoughTime = 500;
const schedule = fn => setTimeout(fn, enoughTime);
const createPayload = () => ({ foo: Date.now() });
const attack = async (amount, publishFire) => {
	await publishFire(createPayload());
	--amount; // eslint-disable-line no-param-reassign
	amount && attack(amount, publishFire); // eslint-disable-line no-unused-expressions
};
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

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

const purgeBySubcriptionId = async (subscriptionId, n) => {
	let activeMessages;
	try {
		activeMessages = await bus.peek(subscriptionId, n);
	} catch (error) {
		console.log(error); // eslint-disable-line no-console
	}
	const processActiveMessages = async () => new Promise((resolve, reject) => {
		const timeout = setTimeout(() => {
			reject();
		}, 5000);

		let count = 0;
		const processMessage = () => {
			count++;
			if (count === activeMessages.length) {
				clearTimeout(timeout);
				resolve();
			}
		};
		const subscribe = () => bus.subscribe(console.error, console.log); // eslint-disable-line no-console
		subscribe()(subscriptionId, processMessage);
	});
	debug(`Peeked ${activeMessages.length} messages in subscriptionId ${subscriptionId}`);
	if (activeMessages.length === 0) return;
	await processActiveMessages();
};

const start = async ({ config }) => {
	debug('Initialising service bus API...');
	bus = await busApi.start({ config });
	return {
		safeSubscribe: bus.subscribe(console.error, console.log), // eslint-disable-line no-console
		publish: bus.publish,
		peekDlq: bus.peekDlq,
		peek: bus.peek,
		purgeDlqBySubcriptionId,
		purgeBySubcriptionId,
		processDlq: bus.processDlq,
		emptyDlq: bus.emptyDlq,
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
	attack,
	sleep,
	bus: { start, stop },
};
