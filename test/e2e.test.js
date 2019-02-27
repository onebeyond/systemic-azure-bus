require('dotenv').config();
const expect = require('expect.js');
const initBus = require('..');

const { start, stop } = initBus();

const stressTopic = 'stress.test';

const config = {
  connection: {
    connectionString: process.env.AZURE_SERVICEBUS_CONNECTION_STRING
  },
  subscriptions: {
    assess: {
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
    }
  }
};

describe('Systemic Azure Bus API', () => {

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

  beforeEach(async () => {
    bus = await start({ config });
    await purgeDlqBySubcriptionId('assess');
  })

  afterEach(async () => {
    await purgeDlqBySubcriptionId('assess');
    await stop();
  })

  it('publishes lots of messages and receives them all', () =>
    new Promise(async (resolve) => {
      const BULLETS = 20;
      const safeSubscribe = bus.subscribe(onError, onStop);
      const publishFire = bus.publish('fire');
      const attack = async (amount) => {
        const shots = Array.from(Array(amount).keys());
        for (shot in shots) {
          await publishFire(createPayload());
        }
      };

      let received = 0;

      const handler = async () => {
        received++;
        if (received === BULLETS) {
          const deadBodies = await bus.peekDlq('assess');
          expect(deadBodies.length).to.equal(0);
          resolve();
        };
      };

      safeSubscribe('assess', handler);
      await attack(BULLETS);
    }));

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
        const deadBodies = await bus.peekDlq('assess');
        expect(deadBodies.length).to.equal(0);
        resolve();
      };

      safeSubscribe('assess', handler);
      await attack();
    }));

  it.skip('retries a message 10 times under the "retry" strategy before going to DLQ', () =>
    new Promise(async (resolve) => {
      const BULLETS = 1;
      const safeSubscribe = bus.subscribe(onError, onStop);
      const publishFire = bus.publish('fire');
      const attack = async (amount) => {
        const shots = Array.from(Array(amount).keys());
        for (shot in shots) {
          await publishFire(createPayload());
        }
      };

      let received = 0;

      const handler = async () => {
        received++;
        console.log(received)
        if (received < 10) throw new Error('Throwing an error to force abandoning the message');
        const deadBodies = await bus.peekDlq('assess');
        expect(deadBodies.length).to.equal(1);
        resolve();
        return true;
      };

      safeSubscribe('assess', handler);
      await attack(BULLETS);
    }));

});