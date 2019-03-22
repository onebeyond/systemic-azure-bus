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

	it('publishes lots of messages, sends them to DLQ and receives them all in DLQ', () => new Promise(async resolve => {
		const BULLETS = 10;
		const publishFire = busApi.publish('fire');
		const attack = async amount => {
			const shots = Array.from(Array(amount).keys());
			for (shot in shots) { // eslint-disable-line guard-for-in,no-restricted-syntax
				await publishFire(createPayload()); // eslint-disable-line no-await-in-loop
			}
		};


		const purgeDlqBySubcriptionId = async () => {
			let receivedMessagesInDLQ = 0;
			const accept = async message => {
				receivedMessagesInDLQ++;
				if (receivedMessagesInDLQ === BULLETS) resolve();
				message.complete();
				return Promise.resolve();
			};
			await busApi.processDlq('assess', accept);
		};

		const handler = async () => {
			const criticalError = new Error('Throwing an error to force going to DLQ');
			criticalError.strategy = 'deadLetter';
			throw criticalError;
		};

		busApi.safeSubscribe('assess', handler);
		await attack(BULLETS);
		schedule(purgeDlqBySubcriptionId);
	}));
});
