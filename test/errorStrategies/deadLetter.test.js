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
    assessWithDlq: {
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

describe('Dead Letter error strategy', () => {

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
    await purgeDlqBySubcriptionId('assessWithDlq');
  });

  afterEach(async () => {
    await purgeDlqBySubcriptionId('assessWithDlq');
    await stop();
  });

  it('sends a message straight to DLQ', () =>
    new Promise(async (resolve) => {
      const safeSubscribe = bus.subscribe(onError, onStop);
      const publishFire = bus.publish('fire');
      const attack = async () => {
        await publishFire(createPayload());
      };

      const confirmDeath = async () => {
        await verifyDeadBody('assessWithDlq');
        resolve();
      };

      const handler = async () => {
        const criticalError = new Error('Throwing an error to force going to DLQ');
        criticalError.strategy = 'deadLetter';
        schedule(confirmDeath);
        throw criticalError;
      };

      safeSubscribe('assessWithDlq', handler);
      await attack();
    }));

});