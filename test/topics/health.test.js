/* eslint-disable no-async-promise-executor */
require('dotenv').config();
const expect = require('expect.js');
const { bus, createPayload } = require('../helper');

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
					subscription: `${assessTopic}.assess`,
				},
			},
			publications: {
				fire: {
					topic: assessTopic,
					contentType: 'application/json',
				},
			},
		};

		beforeEach(async () => {
			busApi = await bus.start({ config });
			await busApi.purgeDlqBySubcriptionId('assess');
		});

		afterEach(async () => {
			await busApi.purgeDlqBySubcriptionId('assess');
			await bus.stop();
		});

		// eslint-disable-next-line no-unused-vars
		it('returns ok if there is messages in the queue', () => new Promise(async (resolve, reject) => { // This must be refactorized
			// should put a message in the topic
			const publish = busApi.publish('fire');

			const handler = async () => {
				const deadBodies = await busApi.peekDlq('assess');
				expect(deadBodies.length).to.equal(0);
				resolve();
			};

			await busApi.safeSubscribe('assess', handler);

			const payload = createPayload();
			await publish(payload);
			const res = await busApi.health();
			expect(res.status).to.eql('ok');
		}));

		it('returns ok always the subscription is reachable/doesnt have any message', async () => {
			// topic for the subscription has to be empty
			const res = await busApi.health();
			expect(res.status).to.eql('ok');
		});
	});
});
