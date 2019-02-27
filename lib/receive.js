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

const onError = logger.error;
const onStop = console.log;
let received = 0;
const AMMONITION = 6000;

const run = async () => {

  const startTime = new Date().getTime();

  const handler = async () => {
    received++;
    if (received % 100 === 0) {
      console.log('Received some!');
    }
    if (received === AMMONITION) {
      const finishTime = new Date().getTime();
      console.log(`Everything received in ${finishTime - startTime} milliseconds`);
      process.exit(0);
      // await stop();
    }
  };

  const { subscribe } = await start({ config, logger });
  const safeSubscribe = subscribe(onError, onStop);
  try {
    safeSubscribe('assess', handler);
  } catch(e) {
    console.error(e);
    await stop();
    process.exit(1);
  }
};

run();