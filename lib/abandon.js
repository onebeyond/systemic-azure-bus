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

const run = async () => {

  const handler = async (body) => {
    console.log('Received some!');
    throw new Error('Throwing an error to force abandoning the message');
  };

  const { subscribe } = await start({ config, logger });
  const safeSubscribe = subscribe(onError, onStop);
  try {
    safeSubscribe('assess', handler);
  } catch(e) {
    console.error(e);
    // await stop();
    process.exit(1);
  }
};

run();