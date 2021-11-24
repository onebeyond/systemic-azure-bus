[![CI](https://github.com/guidesmiths/systemic-azure-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/guidesmiths/systemic-azure-bus/actions/workflows/ci.yml)
[![CD](https://github.com/guidesmiths/systemic-azure-bus/actions/workflows/cd.yml/badge.svg)](https://github.com/guidesmiths/systemic-azure-bus/actions/workflows/cd.yml)

# systemic-azure-bus

Systemic Azure Bus is a [systemic component](https://github.com/guidesmiths/systemic) for the [Azure Service Bus SDK](https://github.com/Azure/azure-sdk-for-js). Its goal is to help you deal with azure bus topics and queues subscriptions and publications.

This library:

* enforces the client to use a particular, sensible configuration
* provides safe defaults for configuration
* Exposes an easy interface for publication/subscription
* Solves error handling
* Allows clients to easily retry, retry with exponential backoff or dead letter a failed message
* Opens/closes the connections

## Configuration

A typical, simple configuration looks like this:

``` js
{
	connection: {
		connectionString: process.env.AZURE_SERVICEBUS_CONNECTION_STRING,
	},
	subscriptions: {
		topicSubscriptionName: {
			topic: 'myTopic',
			subscription: 'myTopic.action'
		},
	},
	publications: {
		topicPublicationName: {
			topic: 'myDestinationTopic',
			contentType: 'application/json', // optional - default is json
		},
	},
}
```

### Systemic API

```js
const initBus = require('systemic-azure-bus');
const { start, stop } = initBus();
...
const api = await start({ config }); // configuration similar to the one above
```

## Topics API

### Publish
```js
const publicationId = 'topicPublicationName'; // declared in config
const publishInMyPublication = api.publish(publicationId);
await publishInMyPublication({ foo: 'bar' });
```

### Subscribe
We provide a streaming API to subscribe to a topic and process messages flowing in.
```js
const subscriptionId = 'topicSubscriptionName'; // declared in config
const subscribe = api.subscribe(console.error); // how to handle error
const handler = ({ body, userProperties }) => {
 // do something with message...
};
subscribe(subscriptionId, handler);
```

### Get Subscription rules
In the case we want to retrieve the rules applied to a subscription, we can use this.

```js
let subscriptionRules = await bus.getSubscriptionRules('topicSubscriptionName');
```

### Peek DLQ
When a message goes to DLQ (Dead Letter Queue) we could peek those messages with this operation.

```js
const subscriptionId = 'topicSubscriptionName'; // declared in config
const deadMessage = await api.peekDlq(subscriptionId); // retrieves only one
```

### Process DLQ
Sometimes we need to process messages in DLQ, i.e. to purge it or to republish and reprocess them. We provide a streaming API to process them.

```js
const handler = ({ body, userProperties }) => {
 // do something with message...
};
const subscriptionId = 'topicSubscriptionName'; // declared in config
api.processDlq(subscriptionId, handler);
```

## Error handling

