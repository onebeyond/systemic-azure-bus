require('dotenv').config();
const expect = require('expect.js');
const { bus, createPayload, schedule, attack, sleep } = require('../helper'); // eslint-disable-line object-curly-newline

const stressTopic = 'stress.test';

const config = {
	connection: {
		connectionString: process.env.AZURE_SERVICEBUS_CONNECTION_STRING,
	},
	subscriptions: {
		assess: {
			topic: stressTopic,
			subscription: 'assess',
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

	before(async () => {
		busApi = await bus.start({ config });
		await busApi.purgeDlqBySubcriptionId('assess');
		await busApi.purgeDlqBySubcriptionId('duplicates');
		await bus.stop();
	});

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

	it('publish a message with explicit messageId and check structure on receiving', () => new Promise(async resolve => {
		const payload = createPayload();
		const messageId = '1234567890';
		const correlationId = 'abc123';
		const publish = busApi.publish('fire');

		const handler = async msg => {
			expect(msg).to.have.keys('body', 'properties', 'applicationProperties');

			const { body, applicationProperties, properties } = msg;
			expect(applicationProperties).to.be.an('object');
			expect(body).to.be.eql(payload);
			expect(properties.messageId).to.be.eql(messageId);
			expect(properties.correlationId).to.be.equal(correlationId);
			resolve();
		};

		busApi.safeSubscribe('assess', handler);
		await publish(payload, { messageId, correlationId });
	}));

	it('publish a message with explicit correlationId and cancel it before receiving', () => new Promise(async (resolve, reject) => {
		const payload = createPayload();
		const publish = busApi.publish('fire');

		let isConsumedMessage;
		const now = Date.now();
		const toConsumeMessageId = `${now}-to-consume-123`;
		const toConsumeCorrelationId = `${now}-to-consume-456`;

		const toCancelMessageId = `${now}-to-cancel-123`;
		const toCancelCorrelationId = `${now}-to-cancel-456`;

		busApi.safeSubscribe('assess', async msg => {
			if (msg.properties.messageId === toConsumeMessageId) {
				isConsumedMessage = toConsumeMessageId;
			}
		});

		busApi.safeSubscribe('assess', async msg => {
			if (msg.properties.messageId === toCancelMessageId) {
				reject(new Error(`Message not cancelled! -> ${msg.properties.correlationId}`));
			}
		});

		// That one needs to get consumed before for making sure sequence numbers are not decreased
		const toConsumeScheduledEnqueueTimeUtc = new Date(Date.now() + 2000);
		await publish(payload, {
			messageId: toConsumeMessageId,
			correlationId: toConsumeCorrelationId,
			scheduledEnqueueTimeUtc: toConsumeScheduledEnqueueTimeUtc,
		});

		const toCancelScheduledEnqueueTimeUtc = new Date(Date.now() + 4000);
		const toCancelSequenceNumber = await publish(payload, {
			messageId: toCancelMessageId,
			correlationId: toCancelCorrelationId,
			scheduledEnqueueTimeUtc: toCancelScheduledEnqueueTimeUtc,
		});

		await busApi.cancelScheduledMessages('fire', toCancelSequenceNumber);
		await sleep(6000);

		expect(isConsumedMessage).to.be.eql(toConsumeMessageId);
		resolve();
	}));

	it('publish a message with explicit messageId and check scheduler on receiving', () => new Promise(async resolve => {
		const payload = createPayload();
		const messageId = '1234567890';
		const publish = busApi.publish('fire');
		// eslint-disable-next-line no-unused-vars
		let startTimestamp;

		const handler = _messageId => async msg => {
			const { properties } = msg;
			if (!properties.messageId || +properties.messageId !== _messageId) return;
			expect((Date.now() - startTimestamp) / 1000).to.be.greaterThan(5);
			expect(msg).to.have.keys('body', 'properties', 'applicationProperties');
			resolve();
		};
		busApi.safeSubscribe('assess', handler(+messageId));
		const scheduledEnqueueTimeUtc = new Date(Date.now() + 5000);
		// eslint-disable-next-line prefer-const
		startTimestamp = Date.now();
		await publish(payload, { messageId, scheduledEnqueueTimeUtc });
	}));

	it(
		'publish a message with explicit messageId and check structure on receiving if the "subscriptionName" is the one',
		() => new Promise(async resolve => {
			const payload = createPayload();
			const messageId = '8734258619';
			const publish = busApi.publish('fire');

			const messagesConsumed = async () => {
				// Not active messages should exists
				const messagesActive = await busApi.peek('assess', 10);
				expect(messagesActive.length).to.be(0);
			};

			const handler = async ({ properties, applicationProperties }) => {
				process.env.HANDLER_EXPECTS_EXECUTED = true;
				expect(applicationProperties.subscriptionName).to.be.eql('assess');
				expect(properties.messageId).to.be.eql(messageId);
			};
			busApi.safeSubscribe('assess', handler);

			await publish(payload, {
				messageId,
				applicationProperties: {
					subscriptionName: 'mocha-test',
				},
			});
			await sleep(2000);
			await messagesConsumed();
			process.env.HANDLER_EXPECTS_EXECUTED = undefined;

			await publish(payload, {
				messageId,
				applicationProperties: {
					subscriptionName: 'assess',
				},
			});
			await sleep(2000);
			// Not active messages should exists
			await messagesConsumed();
			process.env.HANDLER_EXPECTS_EXECUTED = 'true';

			// DLQ should be empty
			const messagesInDlq = await busApi.peekDlq('assess', 10);
			expect(messagesInDlq.length).to.be(0);

			resolve();
		}),
	);

	it('publishes lots of messages with no explicit messageId and receives them all', () => new Promise(async resolve => {
		const BULLETS = 10;
		const publishFire = busApi.publish('fire');

		let received = 0;
		const handler = async msg => {
			received++;
			// expect(msg.properties.messageId.length).to.be.greaterThan(10);
			expect(msg).not.empty();

			if (received === BULLETS) {
				const deadBodies = await busApi.peekDlq('assess');
				expect(deadBodies.length).to.equal(0);
				resolve();
			}
		};

		busApi.safeSubscribe('assess', handler);
		await attack(BULLETS, publishFire);
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

	/**
	 * This test is skipped because the service bus is not filtering duplicated messages on subscriptions
	 */
	it.skip('removes duplicated messages based on messageId', () => new Promise(async resolve => {
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
