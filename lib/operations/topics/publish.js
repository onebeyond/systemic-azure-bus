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
		subject: label,
		applicationProperties: {
			contentEncoding,
			attemptCount: 0,
			...applicationProperties,
		},
		...messageFields,
		body: getBodyEncoded(body, contentEncoding),
	};

	let attempts = 0;
	let lastError = null;
	// Messages not being sent would not get a sequence number assigned
	let sequenceNumber;

	while (!sequenceNumber && attempts < 3) {
		try {
			if (scheduledEnqueueTimeUtc) {
				// eslint-disable-next-line no-await-in-loop
				sequenceNumber = await sender.scheduleMessages([message], scheduledEnqueueTimeUtc);
			} else {
				// eslint-disable-next-line no-await-in-loop
				sequenceNumber = await sender.sendMessages(message);
			}
		} catch (err) {
			lastError = err;
			attempts++;
		}
	}

	if (!sequenceNumber && lastError) throw lastError;

	return Promise.resolve(sequenceNumber);
};
