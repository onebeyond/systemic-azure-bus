require('dotenv').config();
const expect = require('expect.js');
const initBus = require('../..');

const { start, stop } = initBus();

const stressTopic = 'stress.test';
const enoughTime = 500;
const schedule = (fn) => setTimeout(fn, enoughTime);

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

  let bus;
  const onError = console.log;
  const onStop = console.log;

  const createPayload = () => ({ foo: Date.now() });

  const purgeDlqBySubcriptionId = async (subscriptionId) => {
    const accept = async (message) => await message.complete();
    const deadBodies = await bus.peekDlq(subscriptionId);
    if (deadBodies.length === 0) return;
    await bus.processDlq(subscriptionId, accept);
  };

  const verifyDeadBody = async (subscriptionId) => {
    const deadBodies = await bus.peekDlq(subscriptionId);
    expect(deadBodies.length).to.equal(1);
  };

  beforeEach(async () => {
    bus = await start({ config });
    await purgeDlqBySubcriptionId('assessExponentialBackoff');
  });

  afterEach(async () => {
    await purgeDlqBySubcriptionId('assessExponentialBackoff');
    await stop();
  });

  it('retries a message 4 times with exponential backoff before going to DLQ', () =>
    new Promise(async (resolve) => {
      const testedSubscription = 'assessExponentialBackoff';
      let received = 0;
      const safeSubscribe = bus.subscribe(onError, onStop);
      const publishFire = bus.publish('fire');
      const attack = async () => {
        await publishFire(createPayload());
      };

      const confirmDeath = async () => {
        await verifyDeadBody(testedSubscription);
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

      safeSubscribe(testedSubscription, handler);
      await attack();
    }));

});