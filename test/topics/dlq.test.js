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

	it('DLQ peek - should be empty', async () => {
		const messages = await busApi.peekDlq('assess');
		expect(messages.length).to.be(0);
	});

	it('DLQ peek - should contain one message', () => new Promise(async resolve => {
		const BULLETS = 1;
		const publishFire = busApi.publish('fire');
		const attack = async amount => {
			const shots = Array.from(Array(amount).keys());
			for (shot in shots) { // eslint-disable-line guard-for-in,no-restricted-syntax
				await publishFire(createPayload()); // eslint-disable-line no-await-in-loop
			}
		};

		const peekDlq = async () => {
			const firstMessage = await busApi.peekDlq('assess');
			expect(firstMessage.length).to.be(1);
			const moreMessages = await busApi.peekDlq('assess', 1);
			expect(moreMessages.length).to.be(1);
			expect(moreMessages[0].messageId).to.be.eql(firstMessage[0].messageId); // Best approach to test: Second message recovered is equals to first, then its the same (service bus is not working as expected)
			resolve();
		};

		const handler = async () => {
			const criticalError = new Error('Throwing an error to force going to DLQ');
			criticalError.strategy = 'deadLetter';
			throw criticalError;
		};

		busApi.safeSubscribe('assess', handler);
		await attack(BULLETS);
		schedule(peekDlq);
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

		const purgeDlqForSubscription = async () => {
			let receivedMessagesInDLQ = 0;
			const accept = async message => {
				receivedMessagesInDLQ++;
				await message.complete();
				if (receivedMessagesInDLQ === BULLETS) {
					const emptyMessages = await busApi.peekDlq('assess');

					expect(emptyMessages.length).to.be(0);
					resolve();
				}
				return Promise.resolve();
			};
			const allMessages = await busApi.peekDlq('assess', BULLETS);
			expect(allMessages.length).to.be(BULLETS);
			await busApi.processDlq('assess', accept);
		};

		const handler = async () => {
			const criticalError = new Error('Throwing an error to force going to DLQ');
			criticalError.strategy = 'deadLetter';
			throw criticalError;
		};

		busApi.safeSubscribe('assess', handler);
		await attack(BULLETS);
		schedule(purgeDlqForSubscription);
	}));
});
