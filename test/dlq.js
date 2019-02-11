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

const generatePayload = () => ({ foo: Date.now() });

const onError = logger.error;
const onStop = console.log;
const AMMONITION = 10;

const run = async () => {
  const { peekDlq, processDlq, publish, subscribe } = await start({ config, logger });
  const publishFire = publish('fire');
  const safeSubscribe = subscribe(onError, onStop);

  const handler = async (body) => {
    console.log('Received some!', body);
    const e = new Error('Throwing an error to force sending the message straight to DLQ');
    e.strategy = 'dlq';
    throw e;
  };

  const attack = async () => {
    const shots = Array.from(Array(AMMONITION).keys());
    for (shot in shots) {
      await publishFire(generatePayload());
    }
  };

  try {
    // safeSubscribe('assess', handler);
    // await attack();
    let dead = [];
    // while(dead.length < AMMONITION - 1) {
    // }

    // dead = await peekDlq('assess');
    // console.log(dead.length)
    // console.log('process now!')

    await processDlq('assess', async (message) => {
      // console.log(`Received message: ${message.body}`);
      console.log(`Received message:`);
      await message.complete();
    });

    process.exit(0);
  } catch(e) {
    console.error(e);
    // await stop();
    process.exit(1);
  }
};

run();