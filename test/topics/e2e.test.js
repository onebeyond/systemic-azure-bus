require('dotenv').config();
const expect = require('expect.js');
const { bus, createPayload } = require('../helper');

const stressTopic = 'stress.test';

const config = {
	connection: {
		connectionString: process.env.AZURE_SERVICEBUS_CONNECTION_STRING,
	},
	subscriptions: {
		assess: {
			topic: stressTopic,
			subscription: `${stressTopic}.assess`,
			errorHandling: {
				strategy: 'retry',
			},
		},
		duplicates: {
			topic: 'duplicates.test',
			subscription: 'test',
			errorHandling: {
				strategy: 'retry',
			},
		},
	},
	publications: {
		fire: {
			topic: stressTopic,
			contentType: 'application/json',
		},
		duplicates: {
			topic: 'duplicates.test',
			contentType: 'application/json',
		},
	},
};

describe('Topics - Systemic Azure Bus API', () => {
	let busApi;

	beforeEach(async () => {
		busApi = await bus.start({ config });
		await busApi.purgeDlqBySubcriptionId('assess');
	});

	afterEach(async () => {
		await busApi.purgeDlqBySubcriptionId('assess');
		await bus.stop();
	});

	it('publishes lots of messages and receives them all', () => new Promise(async resolve => {
		const BULLETS = 20;
		const publishFire = busApi.publish('fire');
		const attack = async amount => {
			const shots = Array.from(Array(amount).keys());
			for (shot in shots) { // eslint-disable-line guard-for-in,no-restricted-syntax
				await publishFire(createPayload()); // eslint-disable-line no-await-in-loop
			}
		};

		let received = 0;

		const handler = async () => {
			received++;
			if (received === BULLETS) {
				const deadBodies = await busApi.peekDlq('assess');
				expect(deadBodies.length).to.equal(0);
				resolve();
			}
		};

		busApi.safeSubscribe('assess', handler);
		await attack(BULLETS);
	}));

	it('publishes messages with random IDs and receives only non duplicates', () => new Promise(async resolve => {
		const BULLETS = 20;
		const STEPS_FOR_ID_GENERATOR = 5;
		const TARGET = 5;
		const publishFire = busApi.publish('duplicates');

		const getRandomIdGenerator = steps => {
			let currentId = 0;
			let currentSteps = steps;
			return () => {
				currentSteps--;
				if (currentSteps === 0) {
					currentId++;
					currentSteps = steps;
					return currentId;
				}
				return currentId;
			};
		};

		const getRandomId = getRandomIdGenerator(STEPS_FOR_ID_GENERATOR);

		const attack = async amount => {
			const shots = Array.from(Array(amount).keys());
			for (shot in shots) { // eslint-disable-line guard-for-in,no-restricted-syntax
				const id = getRandomId();
				await publishFire(createPayload(), id); // eslint-disable-line no-await-in-loop
			}
		};

		let received = 0;

		const handler = () => received++;

		let checkReceivedAttempt = 0;

		setInterval(async () => {
			if (received === TARGET) {
				checkReceivedAttempt++;
				const deadBodies = await busApi.peekDlq('assess');
				expect(deadBodies.length).to.equal(0);
				if (checkReceivedAttempt === 3) resolve();
			}
		}, 1000);

		busApi.safeSubscribe('duplicates', handler);
		await attack(BULLETS);
	}));
});
