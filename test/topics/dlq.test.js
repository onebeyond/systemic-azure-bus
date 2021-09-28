require('dotenv').config();
const expect = require('expect.js');
const { bus, schedule, attack, sleep } = require('../helper'); // eslint-disable-line object-curly-newline

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

describe('Topics - Systemic Azure Bus API - DLQ', () => {
	let busApi;

	before(async () => {
		busApi = await bus.start({ config });
		await busApi.purgeDlqBySubcriptionId('assess');
		await bus.stop();
	});

	beforeEach(async () => {
		busApi = await bus.start({ config });
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
		const BULLETS = 2;
		const publishFire = busApi.publish('fire');

		const peekDlq = async () => {
			const firstMessage = await busApi.peekDlq('assess');
			expect(firstMessage.length).to.be(1);
			const moreMessages = await busApi.peekDlq('assess', 1);
			expect(moreMessages.length).to.be(1);
			// expect(moreMessages[0].messageId).to.be.eql(firstMessage[0].messageId); // Best approach to test: Second message recovered is equals to first, then its the same (service bus is not working as expected)
			resolve();
		};

		const handler = async () => {
			const criticalError = new Error('Throwing an error to force going to DLQ');
			criticalError.strategy = 'deadLetter';
			throw criticalError;
		};

		busApi.safeSubscribe('assess', handler);
		await attack(BULLETS, publishFire);
		schedule(peekDlq);
	}));

	it('DLQ empty - should empty DLQ after publish a bunch of messages and send them to DLQ', async () => {
		const BULLETS = 20;
		const publishFire = busApi.publish('fire');

		const handler = async () => {
			const criticalError = new Error('Throwing an error to force going to DLQ');
			criticalError.strategy = 'deadLetter';
			throw criticalError;
		};

		busApi.safeSubscribe('assess', handler);
		await attack(BULLETS, publishFire);
		await sleep(8000); // needed for correct peek
		const messagesInDlq = await busApi.peekDlq('assess', BULLETS);

		expect(messagesInDlq.length).to.be(BULLETS);

		await busApi.emptyDlq('assess');

		const messagesInDlqAfterEmptying = await busApi.peekDlq('assess', BULLETS);

		expect(messagesInDlqAfterEmptying.length).to.be(0);
	});

	it.skip('publishes lots of messages, sends them to DLQ and receives them all in DLQ', () => new Promise(async resolve => {
		const BULLETS = 10;
		const publishFire = busApi.publish('fire');

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
		await attack(BULLETS, publishFire);
		schedule(purgeDlqForSubscription);
	}));
});
