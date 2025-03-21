<!-- # sqs-emitter

![Build Status](https://github.com/janis-commerce/sqs-emitter/workflows/Build%20Status/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/janis-commerce/sqs-emitter/badge.svg?branch=false)](https://coveralls.io/github/janis-commerce/sqs-emitter?branch=false)
[![npm version](https://badge.fury.io/js/%40janiscommerce%2Fsqs-emitter.svg)](https://www.npmjs.com/package/@janiscommerce/sqs-emitter)



## Installation
```sh
npm install @janiscommerce/sqs-emitter
```

## API


## Usage
```js
const SqsEmitter = require('@janiscommerce/sqs-emitter');

```

## Examples -->

# sqs-emitter

![Build Status](https://github.com/janis-commerce/sqs-emitter/workflows/Build%20Status/badge.svg)
[![npm version](https://badge.fury.io/js/%40janiscommerce%2Fsqs-emitter.svg)](https://www.npmjs.com/package/@janiscommerce/sqs-emitter)

SQS Wrapper

## Installation
```sh
npm install @janiscommerce/sqs-emitter
```

### Install peer dependencies
```sh
# Install as devDependency if you run your code in AWS Lambda, which already includes the SDK
npm install --dev @aws-sdk/client-sqs@3
```

> Why? This is to avoid installing the SDK in production and freezing the SDK version in this package

### SQS Emitter

> This class is compatible with [@janiscommerce/api-session](https://npmjs.com/@janiscommerce/api-session). If it's instanciated using `getSessionInstance`, a message attribute `janis-client` with the session's `clientCode` will be automatically added to every event.

> The event `content` will be JSON-stringified before sending

> The event `attributes` can be either Strings or Arrays.  It's important to note that using other data types may cause issues or inconsistencies in the implemented filter policies. Ensure that the values provided for the attributes are always of the expected type to avoid errors in message processing.

> The `payloadFixedProperties` event must be an array of strings containing the properties that must be mandatorily sent in the content. This is to improve error management, as these properties will allow us to identify which data failed and make a decision accordingly.

**!important:** The session is required to obtain the `clientCode` and construct the `contentS3Path` for payloads that exceed the maximum SQS message size limit.

#### Publish single event

```js
const { SqsEmitter } = require('@janiscommerce/sqs-emitter');

const sqsEmitter = this.session.getSessionInstance(SqsEmitter);

const result = await sqsEmitter.publishEvent('http://sqs-url', {
  content: {
    id: '1'
  },
  attributes: {
    source: 'user',
    platforms: ['mobile', 'web']
  },
  payloadFixedProperties: ['id']
});

/**
 * Sample Output
 *
 * {
 * 	MessageId: '8563a94f-59f3-4843-8b16-a012867fe97e',
 * 	SequenceNumber: '' // For FIFO topics only
 * }
 */
```

#### Publish multiple events

> This method will send multiple events in one SDK call. It will also separate in batches when the total size limit of 256KB payload size is exceeded. Batches will be sent with a smart concurrency protocol (optimizing calls with a maximum of 25 concurrent calls).

```js
const { SqsEmitter } = require('@janiscommerce/sqs-emitter');

const sqsEmitter = this.session.getSessionInstance(SqsEmitter);

const result = await sqsEmitter.publishEvents('http://sqs-url', [
  {
    content: {
      id: '1'
    },
    attributes: {
      source: 'user',
      platform: 'mobile'
    },
    payloadFixedProperties: ['id']
  },
  {
    content: {
      id: '2'
    },
    attributes: {
      source: 'user',
      platform: 'mobile'
    },
    payloadFixedProperties: ['id']
  }
]);

/**
 * Sample Output
 *
 * {
 * 	successCount: 1,
 * 	failedCount: 1,
 * 	results: [
 * 		{
 * 			success: true,
 *				messageId: '8563a94f-59f3-4843-8b16-a012867fe97e'
 * 		},
 * 		{
 * 			success: false,
 * 			errorCode: '',
 * 			errorMessage: ''
 * 		}
 * 	]
 * }
 */
```
