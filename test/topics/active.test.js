require('dotenv').config();
const expect = require('expect.js');
const { bus, schedule, attack } = require('../helper');

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
		await busApi.purgeBySubcriptionId('assess');
	});

	afterEach(async () => {
		await busApi.purgeBySubcriptionId('assess');
		await bus.stop();
	});

	it('Active peek - should be empty', async () => {
		const messages = await busApi.peek('assess', 1);
		expect(messages.length).to.be(0);
	});

	it('Active peek - should contain three messages', () => new Promise(async resolve => {
		const BULLETS = 3;
		const publishFire = busApi.publish('fire');

		const peek = async () => {
			const peekedMessages = await busApi.peek('assess', 3);
			expect(peekedMessages.length).to.be(3);
			await busApi.purgeBySubcriptionId('assess', 3);
			resolve();
		};

		await attack(BULLETS, publishFire);
		schedule(peek);
	}));
});
