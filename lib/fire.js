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
  }
};

const payload = { foo: 'bar' };

const AMMONITION = 6000;

const run = async () => {

  const startTime = new Date().getTime();

  const attack = async () => {
    const shots = Array.from(Array(AMMONITION).keys());
    for (shot in shots) {
      await publishFire(payload);
    }
  };

  const { publish } = await start({ config, logger });
  const publishFire = publish('fire');
  try {
    await attack();
    const publicationTime = new Date().getTime();
    console.log(`Everything published in ${publicationTime - startTime} milliseconds`);
    process.exit(0);
  } catch(e) {
    console.error(e);
    await stop();
    process.exit(1);
  }
};

run();