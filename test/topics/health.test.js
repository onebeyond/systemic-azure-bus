require('dotenv').config();
const expect = require('expect.js');
const { bus, createPayload, schedule } = require('../helper');

const assessTopic = 'stress.test';

describe('Health check', () => {
	let busApi;

	describe('for subscription', () => {
		const config = {
			connection: {
				connectionString: process.env.AZURE_SERVICEBUS_CONNECTION_STRING,
			},
			subscriptions: {
				assess: {
					topic: assessTopic,
					subscription: 'assess',
				},
			},
			publications: {
				fire: {
					topic: assessTopic,
					contentType: 'application/json',
				},
			},
		};

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
		});

		// eslint-disable-next-line no-unused-vars
		it('returns ok if there is messages in the queue', () => new Promise(async (resolve, reject) => { // This must be refactorized
			// should put a message in the topic
			const publish = busApi.publish('fire');

			const dlqMessages = async () => {
				const deadBodies = await busApi.peekDlq('assess');
				expect(deadBodies.length).to.equal(1);
				resolve();
			};

			await busApi.safeSubscribe('assess');

			const payload = createPayload();
			await publish(payload);
			const res = await busApi.health();
			expect(res.status).to.eql('ok');
			schedule(dlqMessages);
		}));

		it('returns ok always the subscription is reachable/doesnt have any message', async () => {
			// topic for the subscription has to be empty
			const res = await busApi.health();
			expect(res.status).to.eql('ok');
		});
	});
});
