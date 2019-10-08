require('dotenv').config();
const expect = require('expect.js');
const { bus, createPayload, schedule } = require('../helper');

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
	},
	publications: {
		fire: {
			topic: stressTopic,
			contentType: 'application/json',
		},
	},
};

describe('Topics - Systemic Azure Bus API', () => {
	let busApi;

	beforeEach(async () => {
		busApi = await bus.start({ config });
		await busApi.purgeActiveBySubcriptionId('assess');
	});

	afterEach(async () => {
		await busApi.purgeActiveBySubcriptionId('assess');
		await bus.stop();
	});

	it('Active peek - should be empty', async () => {
		const messages = await busApi.peekActive('assess', 1);
		expect(messages.length).to.be(0);
	});

	it('Active peek - should contain three message', () => new Promise(async resolve => {
		const BULLETS = 3;
		const publishFire = busApi.publish('fire');
		const attack = async amount => {
			const shots = Array.from(Array(amount).keys());
			for (shot in shots) { // eslint-disable-line guard-for-in,no-restricted-syntax
				await publishFire(createPayload()); // eslint-disable-line no-await-in-loop
			}
		};

		const peekActive = async () => {
			const peekedMessages = await busApi.peekActive('assess', 3);
			expect(peekedMessages.length).to.be(3);
			await busApi.purgeActiveBySubcriptionId('assess', 3);
			resolve();
		};

		await attack(BULLETS);
		schedule(peekActive);
	}));
});
