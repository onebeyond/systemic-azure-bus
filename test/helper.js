const debug = require('debug')('systemic-azure-bus:test:helper');

const initBus = require('..');
const busApi = initBus();

const enoughTime = 500;
const schedule = (fn) => setTimeout(fn, enoughTime);
const createPayload = () => ({ foo: Date.now() });

let bus;

const purgeDlqBySubcriptionId = async (subscriptionId) => {
  const accept = async (message) => await message.complete();
  const deadBodies = await bus.peekDlq(subscriptionId);
  if (deadBodies.length === 0) return;
  await bus.processDlq(subscriptionId, accept);
};

const checkDeadBodies = async (subscriptionId) => {
  const deadBodies = await bus.peekDlq(subscriptionId);
  return deadBodies;
};

const start = async ({ config }) => {
  debug('Initialising service bus API...');
  bus = await busApi.start({ config });
  return {
    safeSubscribe: bus.subscribe(console.error, console.log),
    publish: bus.publish,
    purgeDlqBySubcriptionId,
    checkDeadBodies,
  };
};

const stop = async () => {
  debug('Stopping service bus API...');
  await busApi.stop();
};

module.exports = {
  createPayload,
  schedule,
  bus: { start, stop }
};