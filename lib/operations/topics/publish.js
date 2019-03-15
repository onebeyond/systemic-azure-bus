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
	await sender.send(message);
};
