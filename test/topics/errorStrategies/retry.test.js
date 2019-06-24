require('dotenv').config();
const expect = require('expect.js');
const { bus, createPayload, schedule } = require('../../helper');

const stressTopic = 'stress.test';

const config = {
	connection: {
		connectionString: process.env.AZURE_SERVICEBUS_CONNECTION_STRING,
	},
	subscriptions: {
		assessWithRetry: {
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

describe('Topics - Retry error strategy', () => {
	let busApi;

	beforeEach(async () => {
		busApi = await bus.start({ config });
		await busApi.purgeDlqBySubcriptionId('assessWithRetry');
	});

	afterEach(async () => {
		await busApi.purgeDlqBySubcriptionId('assessWithRetry');
		await bus.stop();
	});

	it('retries a message and recovers it under the "retry" strategy not going to DLQ', () => new Promise(async resolve => {
		const publishFire = busApi.publish('fire');
		let received = 0;

		const handler = async () => {
			received++;
			if (received < 2) throw new Error('Throwing an error to force abandoning the message');
			expect(received).to.equal(3);
			const deadBodies = await busApi.peekDlq('assessWithRetry');
			expect(deadBodies.length).to.equal(0);
			resolve();
		};

		busApi.safeSubscribe('assessWithRetry', handler);
		await publishFire(createPayload());
	}));

	it.skip('retries a message 10 times under the "retry" strategy before going to DLQ', () => new Promise(async resolve => {
		let received = 0;
		const publishFire = busApi.publish('fire');

		const confirmDeath = async () => {
			const deadBodies = await busApi.peekDlq('assessWithRetry');
			expect(deadBodies.length).to.equal(1);
			resolve();
		};

		const handler = async () => {
			received++;
			if (received === 10) {
				schedule(confirmDeath);
				throw new Error('Throwing the last error to end up in DLQ');
			} else {
				throw new Error('Throwing an error to force abandoning the message');
			}
		};

		busApi.safeSubscribe('assessWithRetry', handler);
		await publishFire(createPayload());
	}));
});
