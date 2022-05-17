const zlib = require('zlib');

const encodingStrategies = {
	zlib: body => {
		const bodyStringified = JSON.stringify(body);
		return zlib.deflateSync(Buffer.from(bodyStringified));
	},
	default: body => body,
};

const getBodyEncoded = (body, contentEncoding) => (encodingStrategies[contentEncoding] || encodingStrategies.default)(body);

module.exports = sender => async (body, options = {}) => { // eslint-disable-line object-curly-newline
	const { label = '', contentEncoding, applicationProperties, scheduledEnqueueTimeUtc, ...messageFields } = options;
	const message = {
		label,
		applicationProperties: {
			contentEncoding,
			attemptCount: 0,
			...applicationProperties,
		},
		...messageFields,
		body: getBodyEncoded(body, contentEncoding),
	};

	let isMessageSent = false;
	let attempts = 0;
	let lastError = null;

	while (!isMessageSent && attempts < 3) {
		try {
			if (scheduledEnqueueTimeUtc) {
				// eslint-disable-next-line no-await-in-loop
				await sender.scheduleMessages([message], scheduledEnqueueTimeUtc);
			} else {
				// eslint-disable-next-line no-await-in-loop
				await sender.sendMessages(message);
			}
			isMessageSent = true;
		} catch (err) {
			lastError = err;
			attempts++;
		}
	}

	if (!isMessageSent && lastError) throw lastError;
};
