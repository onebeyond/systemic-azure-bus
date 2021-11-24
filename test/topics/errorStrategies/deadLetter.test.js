require('dotenv').config();
const expect = require('expect.js');
const { bus, createPayload, schedule, sleep } = require('../../helper');

const stressTopic = 'stress.test';

const config = {
	connection: {
		connectionString: process.env.AZURE_SERVICEBUS_CONNECTION_STRING,
	},
	subscriptions: {
		assessWithDlq: {
			topic: stressTopic,
			subscription: 'assess',
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

// TODO: Have a look at this together. I Had to skip these because they sometimes work and sometimes don't ¯\_(ツ)_/¯
describe('Topics - Dead Letter error strategy', () => {
	let busApi;

	before(async () => {
		busApi = await bus.start({ config });
		await busApi.purgeDlqBySubcriptionId('assessWithDlq');
		await bus.stop();
	});

	beforeEach(async () => {
		busApi = await bus.start({ config });
	});

	afterEach(async () => {
		await busApi.purgeDlqBySubcriptionId('assessWithDlq');
		await bus.stop();
	});

	it('sends a message straight to DLQ', () => new Promise(async resolve => {
		const publishFire = busApi.publish('fire');
		const attack = async () => {
			await publishFire(createPayload());
		};

		const confirmDeath = async () => {
			const deadBodies = await busApi.peekDlq('assessWithDlq');
			expect(deadBodies.length).to.equal(1);
			await sleep(4000); // needed for correct peek
			resolve();
		};

		const handler = async () => {
			const criticalError = new Error('Throwing an error to force going to DLQ');
			criticalError.strategy = 'deadLetter';
			schedule(confirmDeath);
			throw criticalError;
		};

		busApi.safeSubscribe('assessWithDlq', handler);
		await attack();
	}));
});
