const publish = require('../../lib/operations/topics/publish');

const createPayload = () => ({ foo: Date.now() });


const createSender = (maxAttempts = 2) => {
	let attempts = 0;

	const send = async () => {
		if (attempts < (maxAttempts - 1)) attempts++;
		else return Promise.resolve();
		throw new Error();
	};

	return {
		send,
	};
};


describe('Publish  message on Topic', () => {
	it('Should publish', () => new Promise(async resolve => {
		const sender = createSender(3);
		const publishMessage = publish(sender);
		await publishMessage(createPayload());
		resolve();
	}));

	it('Should not publish', () => new Promise(async resolve => {
		const sender = createSender(5);
		const publishMessage = publish(sender);
		try {
			await publishMessage(createPayload());
		} catch (err) {
			resolve();
		}
	}));
});
