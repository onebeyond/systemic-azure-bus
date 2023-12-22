[![CI](https://github.com/guidesmiths/systemic-azure-bus/actions/workflows/ci.yml/badge.svg)](https://github.com/guidesmiths/systemic-azure-bus/actions/workflows/ci.yml)
[![CD](https://github.com/guidesmiths/systemic-azure-bus/actions/workflows/cd.yml/badge.svg)](https://github.com/guidesmiths/systemic-azure-bus/actions/workflows/cd.yml)

# systemic-azure-bus

Systemic Azure Bus is a [systemic component](https://github.com/guidesmiths/systemic) for the [Azure Service Bus SDK](https://github.com/Azure/azure-sdk-for-js). Its goal is to help you deal with azure bus topics and queues subscriptions and publications.

This library:

* Enforces the client to use a particular, sensible configuration
* Provides safe defaults for configuration
* Exposes an easy interface for publication/subscription
* Solves error handling
* Allows clients to easily retry, retry with exponential backoff or dead letter a failed message
* Opens/closes the connections

## Configuration

A typical, simple configuration looks like this:

``` js
{
	connection: {
		connectionString: process.env.AZURE_SERVICE_BUS_CONNECTION_STRING,
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

Names that will included in the `topic` and `subscription` properties inside each publication / subscription object must match existing ones in your Azure service bus instance.

As for the properties `topicSubscriptionName` and `topicPublicationName` in this example, those are mere samples and can be fully customized based on your needs. Names you add to all objects you create inside of `subscriptions` and `publications` are the names that you'll use to reference a specific publication / subscription from within your source code. You can add as many topics as you need in each category.

### Systemic API

```js
const initBus = require('systemic-azure-bus');
const { start, stop } = initBus();
...
const api = await start({ config }); // configuration similar to the one above
```

## Topics API

### Publish

#### Immediatly publishing a message

In order to be able to publish messages in the service bus, the `publish` method should be used to generate a function that can be called anytime to publish messages to a specific topic. We can immediatly publish a message by passing the message contents to the method returned by `publish` method, as illustrated in the example below:

```js
// Generate method to publish messages in a specific topic
const publicationId = 'topicPublicationName'; // declared in config
const publishInMyPublication = api.publish(publicationId);

// Publish a message to be consumed as soon as possible
await publishInMyPublication(messageBody);
```

#### Schedule a message to be published at a specific moment in the future

The function returned by the `publish` method can optionally receive a second parameter with an options object. The option `scheduledEnqueueTimeUtc` can be used to delay the publication of a message so it's not consumed immediatly. Instead, message will be kept on the queue and published at an specific date.

```js
// Generate method to publish messages in a specific topic
const publicationId = 'topicPublicationName'; // declared in config
const publishInMyPublication = api.publish(publicationId);

// Schedule a message to be published at a future date
const delayedPublishDate = new Date(Date.now() + 2000);
await publishInMyPublication(messageBody, { scheduledEnqueueTimeUtc: delayedPublishDate });
```

### Cancel scheduled messages

A message planned to be published on a future date can be cancelled anytime by using the `cancelScheduledMessages` method. We can use this if our scheduled message became obsolete or it's no longer needed for whichever reason, so we prevent it from being sent to the subscribers.

To cancel a message it's necessary that we provide the id of a message, which we can obtain from the return value of the function used to publish the message.

```js
// Generate method to publish messages in a specific topic
const publicationId = 'topicPublicationName'; // declared in config
const publishInMyPublication = api.publish(publicationId);

// Schedule a message to be published at a future date and store its id
const messageId = await publishInMyPublication(messageBody, { scheduledEnqueueTimeUtc: delayedPublishDate });

// Cancel the message
await api.cancelScheduledMessages(publicationId, messageId);
```

### Subscribe
We provide a streaming API to subscribe to a topic and process messages flowing in. The example below illustrates how to use the `subscribe` method to be able to listen and process messages on a given topic.

```js
const subscriptionId = 'topicSubscriptionName'; // declared in config

// We need to define a function that will process messages received on this subscription
// The `body` property will contain the actual contents of the message that were published to the service bus
const handleReceivedMessage = ({ body, userProperties }) => {
 	// do something with message...
};

// We also need to define an error handling function that'll run in case we fail to process the message
const onMessageProcessingError = console.error;

// Start listening to messages in the configured subscription and determine functions that'll be used to handle incoming messages
const subscribe = api.subscribe(onMessageProcessingError);
subscribe(subscriptionId, handleReceivedMessage);
```

### Get Subscription rules
In the case we want to retrieve the rules applied to a subscription, we can use this.

```js
let subscriptionRules = await bus.getSubscriptionRules('topicSubscriptionName');
```

### Peek DLQ
When a message goes to DLQ (Dead Letter Queue) we can peek those messages with this operation.

```js
const subscriptionId = 'topicSubscriptionName'; // declared in config
const deadMessage = await api.peekDlq(subscriptionId); // retrieves only one
```

### Process DLQ
Sometimes we need to process messages in DLQ, i.e. to purge it or to republish and reprocess them. We provide a streaming API to process them.

```js
// Define function to handle messages
const handleReceivedMessage = ({ body, userProperties }) => {
 	// do something with message...
};

const subscriptionId = 'topicSubscriptionName'; // declared in config
api.processDlq(subscriptionId, handleReceivedMessage);
```
