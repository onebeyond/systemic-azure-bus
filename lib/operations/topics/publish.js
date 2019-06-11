const zlib = require('zlib');

const encodingStrategies = {
	zlib: body => {
		const bodyStringified = JSON.stringify(body);
		return zlib.deflateSync(Buffer.from(bodyStringified));
	},
	default: body => body,
};

const getBodyEncoded = (body, contentEncoding) => (encodingStrategies[contentEncoding] || encodingStrategies.default)(body);

module.exports = sender => async (body, { messageId, label = '', contentType, contentEncoding, userProperties } = {}) => { // eslint-disable-line object-curly-newline
	const message = {
		body: getBodyEncoded(body, contentEncoding),
		messageId,
		label,
		contentType,
		userProperties: {
			contentEncoding,
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
