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
		await busApi.purgeDlqBySubcriptionId('duplicates');
	});

	afterEach(async () => {
		await busApi.purgeDlqBySubcriptionId('assess');
		await busApi.purgeDlqBySubcriptionId('duplicates');
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

	it('publish a message encoded with zlib and decodes it properly', () => new Promise(async resolve => {
		const payload = createPayload();

		const handler = async message => {
			expect(message.body).to.eql(payload);
			resolve();
		};

		busApi.safeSubscribe('assess', handler);
		await busApi.publish('fire')(payload, { contentEncoding: 'zlib' }); // eslint-disable-line no-await-in-loop
	}));


	it('removes duplicated messages based on messageId', () => new Promise(async resolve => {
		const STEPS_FOR_ID_GENERATOR = 5;
		const MESSAGES_TO_SEND = 20;
		const EFFECTIVE_MESSAGES = 5;
		const publishDups = busApi.publish('duplicates');
		const receivedMessages = [];

		const dupIdGenerator = steps => {
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
		const getRandomDupId = dupIdGenerator(STEPS_FOR_ID_GENERATOR);

		const publishMessages = async () => {
			const shots = Array.from(Array(MESSAGES_TO_SEND).keys());
			for (shot in shots) { // eslint-disable-line guard-for-in,no-restricted-syntax
				const messageId = getRandomDupId();
				const payload = createPayload();
				await publishDups(payload, { messageId }); // eslint-disable-line no-await-in-loop
			}
		};

		const checkMessages = async () => {
			expect(receivedMessages.length).to.be.eql(EFFECTIVE_MESSAGES);

			const deadBodies = await busApi.peekDlq('duplicates');
			expect(deadBodies.length).to.equal(0);
			resolve();
		};

		const handler = msg => receivedMessages.push(msg);
		busApi.safeSubscribe('duplicates', handler);

		await publishMessages();
		schedule(checkMessages);
	}));
});
