require('dotenv').config();
const initBus = require('..');

const { start, stop } = initBus();

if (!process.env.AZURE_SERVICEBUS_CONNECTION_STRING) {
  throw new Error('Please place a valid AZURE_SERVICEBUS_CONNECTION_STRING variable in your .env file');
}

const logger = {
  error: console.log
};

const stressTopic = 'stress.test';

const config = {
  publications: {
    fire: {
      topic: stressTopic,
    }
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
};

const payload = { foo: 'bar' };

const onError = logger.error;
const onStop = console.log;
let received = 0;
const AMMONITION = 10;

const run = async () => {

  const startTime = new Date().getTime();

  const handler = async (body) => {
    received++;
    if (received % 100 === 0) {
      console.log('Received some!');
    }
    if (received === AMMONITION) {
      const finishTime = new Date().getTime();
      console.log(`Everything received in ${finishTime - startTime} milliseconds`);
      // await stop();
    }
    return true;
  };

  const attack = async () => {
    const shots = Array.from(Array(AMMONITION).keys());
    for (shot in shots) {
      await publishFire(payload);
    }
  };

  const { publish, subscribe } = await start({ config, logger });
  const publishFire = publish('fire');
  const safeSubscribe = subscribe(onError, onStop);
  try {
    safeSubscribe('assess', handler);
    await attack();
    const publicationTime = new Date().getTime();
    console.log(`Everything published in ${publicationTime - startTime} milliseconds`);
  } catch(e) {
    console.error(e);
    await stop();
    process.exit(1);
  }
};

run();