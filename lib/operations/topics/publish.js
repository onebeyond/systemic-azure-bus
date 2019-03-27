module.exports = sender => async (body, label = '', contentType, userProperties) => {
	const message = {
		body,
		label,
		contentType,
		userProperties: {
			...userProperties,
			attemptCount: 0,
		},
	};

	let isMessageSent = false;
	let attempts = 0;
	let lastError = null;

	while (!isMessageSent && attempts < 3) {
		try {
			await sender.send(message); // eslint-disable-line no-await-in-loop
			isMessageSent = true;
		} catch (err) {
			lastError = err;
			attempts++;
		}
	}

	if (!isMessageSent && lastError) throw lastError;
};
