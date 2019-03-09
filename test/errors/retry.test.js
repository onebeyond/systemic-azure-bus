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
    assessWithRetry: {
      topic: stressTopic,
      subscription: `${stressTopic}.assess`,
      errorHandling: {
        strategy: 'retry'
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

describe('Retry error strategy', () => {

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
    await purgeDlqBySubcriptionId('assessWithRetry');
  });

  afterEach(async () => {
    await purgeDlqBySubcriptionId('assessWithRetry');
    await stop();
  });

  it('retries a message and recovers it under the "retry" strategy not going to DLQ', () =>
    new Promise(async (resolve) => {
      const safeSubscribe = bus.subscribe(onError, onStop);
      const publishFire = bus.publish('fire');
      const attack = async () => {
        await publishFire(createPayload());
      };

      let received = 0;

      const handler = async () => {
        received++;
        if (received < 2) throw new Error('Throwing an error to force abandoning the message');
        expect(received).to.equal(3);
        const deadBodies = await bus.peekDlq('assessWithRetry');
        expect(deadBodies.length).to.equal(0);
        resolve();
      };

      safeSubscribe('assessWithRetry', handler);
      await attack();
    }));

  it('retries a message 10 times under the "retry" strategy before going to DLQ', () =>
    new Promise(async (resolve) => {
      let received = 0;
      const safeSubscribe = bus.subscribe(onError, onStop);
      const publishFire = bus.publish('fire');
      const attack = async () => {
        await publishFire(createPayload());
      };

      const confirmDeath = async () => {
        await verifyDeadBody('assessWithRetry');
        resolve();
      };

      const handler = async () => {
        received++;
        if (received === 10) {
          schedule(confirmDeath);
          throw new Error('Throwing the last error to end up in DLQ');
        } else {
          throw new Error('Throwing an error to force abandoning the message');
        }
      };

      safeSubscribe('assessWithRetry', handler);
      await attack();
    }));

});