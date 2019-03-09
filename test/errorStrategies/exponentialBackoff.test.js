require('dotenv').config();
const expect = require('expect.js');
const { bus, createPayload, schedule } = require('../helper');

const stressTopic = 'stress.test';

const config = {
  connection: {
    connectionString: process.env.AZURE_SERVICEBUS_CONNECTION_STRING
  },
  subscriptions: {
    assessExponentialBackoff: {
      topic: stressTopic,
      subscription: `${stressTopic}.assess`,
      errorHandling: {
        strategy: 'exponentialBackoff',
        options: {
          measure: 'seconds',
          attempts: 4 // Maximum 10
        }
      }
    },
  },
  publications: {
    fire: {
      topic: stressTopic,
      contentType: 'application/json'
    }
  }
};

describe('Exponential Backoff error strategy', () => {

  let busApi;

  beforeEach(async () => {
    busApi = await bus.start({ config });
    await busApi.purgeDlqBySubcriptionId('assessExponentialBackoff');
  });

  afterEach(async () => {
    await busApi.purgeDlqBySubcriptionId('assessExponentialBackoff');
    await bus.stop();
  });

  it('retries a message 4 times with exponential backoff before going to DLQ', () =>
    new Promise(async (resolve) => {
      const testedSubscription = 'assessExponentialBackoff';
      let received = 0;
      const publishFire = busApi.publish('fire');
      const attack = async () => {
        await publishFire(createPayload());
      };

      const confirmDeath = async () => {
        const deadBodies = await busApi.checkDeadBodies(testedSubscription);
        expect(deadBodies.length).to.equal(1);
        resolve();
      };

      const expBackoffConfig = config.subscriptions[testedSubscription];
      const maxAttempts = expBackoffConfig.errorHandling.options.attempts;

      const handler = async ({ userProperties }) => {
        expect(userProperties.attemptCount).to.equal(received);
        received++;
        if (received === maxAttempts) {
          schedule(confirmDeath);
          throw new Error('Throwing the last error to end up in DLQ');
        } else {
          throw new Error('Throwing an error to force abandoning the message');
        }
      };

      busApi.safeSubscribe(testedSubscription, handler);
      await attack();
    }));

});