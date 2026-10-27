# sqs-emitter

![Build Status](https://github.com/janis-commerce/sqs-emitter/workflows/Build%20Status/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/janis-commerce/sqs-emitter/badge.svg?branch=master)](https://coveralls.io/github/janis-commerce/sqs-emitter?branch=master)
[![npm version](https://badge.fury.io/js/%40janiscommerce%2Fsqs-emitter.svg)](https://www.npmjs.com/package/@janiscommerce/sqs-emitter)

## Installation

```sh
npm install @janiscommerce/sqs-emitter
```

## Breaking changes ⚠️

### 1.0.0
- When using this package with serverless, it's **mandatory** to use [`sls-helper-plugin-janis`](https://www.npmjs.com/package/sls-helper-plugin-janis) version **10.2.0** or higher to handle messages that exceed the SNS payload limit. This version is **required** to ensure proper permissions are set up.
- Additionally, it's **mandatory** to update [`@janiscommerce/sqs-consumer`](https://www.npmjs.com/package/@janiscommerce/sqs-consumer) to version **1.0.0** or higher in any service that listens to events emitted by this package. This way, storage and retrieval of large payloads through S3 will be automatically handled when needed.

## Usage

```js
const { SqsEmitter } = require('@janiscommerce/sqs-emitter');
```

### SQS Emitter

> This class is compatible with [@janiscommerce/api-session](https://npmjs.com/@janiscommerce/api-session). If it's instanciated using `getSessionInstance`, a message attribute `janis-client` with the session's `clientCode` will be automatically added to every event.

> The event `content` will be JSON-stringified before sending

> The event `attributes` can be either Strings or Arrays.  It's important to note that using other data types may cause issues or inconsistencies in the implemented filter policies. Ensure that the values provided for the attributes are always of the expected type to avoid errors in message processing.

> The `payloadFixedProperties` property must be an array of strings specifying the content properties that must be mandatorily sent. This improves error management by enabling us to identify which data failed and decide accordingly.

#### Publish single event

```js
const { SqsEmitter } = require('@janiscommerce/sqs-emitter');

const sqsEmitter = this.session.getSessionInstance(SqsEmitter);

const result = await sqsEmitter.publishEvent('https://sqs.us-east-1.amazonaws.com/123456789012/MySQSName', {
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
 *  messageId: '8563a94f-59f3-4843-8b16-a012867fe97e',
 *  sequenceNumber: '18889007921482248832' // For FIFO queues only
 * }
 */
```

> If the event exceeds the 256KB limit and the payload can't be uploaded to any S3 bucket, `publishEvent` rejects with a `SqsEmitterError` with code `S3_ERROR` and nothing is sent to SQS.

#### Publish multiple events

> This method will send multiple events in one SDK call. It will also separate in batches when the total size limit of 256KB payload size is exceeded. Batches will be sent with a smart concurrency protocol (optimizing calls with a maximum of 25 concurrent calls).

```js
const { SqsEmitter } = require('@janiscommerce/sqs-emitter');

const sqsEmitter = this.session.getSessionInstance(SqsEmitter);

const result = await sqsEmitter.publishEvents('https://sqs.us-east-1.amazonaws.com/123456789012/MySQSName', [
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
 *   successCount: 1,
 *   failedCount: 1,
 *   success: [
 *   {
 *    Id: '1',
 *    messageId: '8563a94f-59f3-4843-8b16-a012867fe97e'
 *   }
 *  ],
 *  failed: [
 *   {
 *    Id: '2',
 *    SenderFault: true,
 *    Code: 'SQS001',
 *    Message: 'SQS Failed'
 *   }
 *  ]
 * }
 */
```

> `failed` entries rejected by SQS keep the SQS format (`Id`, `SenderFault`, `Code`, `Message`). Events exceeding 256KB whose payload can't be uploaded to any S3 bucket are not sent and are reported as `{ Id, Code: 'S3_ERROR', Message: 'Failed to upload to all provided s3 buckets' }`.

## Logging 🔎

Events that fail to be published are no longer silent: `publishEvents()` logs them **by default**, so they show up in your function logs even if your code doesn't check the `failed` entries of the response.

- 📝 A single `logger.error` per call, only when at least one event failed.
- 📊 It includes the queue name, the `clientCode` (when the emitter has a session), the failed count and the total count.
- 🔬 `failedSample` holds up to the **first 10** failed entries, with only `Id`, `Code`, `Message` and `SenderFault`.
- 🔒 The message content (`MessageBody`, `content`, attributes) is **never** logged, as it may contain personal data.

```js
// Sample log metadata
{
  sqsName: 'MySQSName',
  clientCode: 'my-client',
  failedCount: 25,
  totalCount: 200,
  failedSample: [
    { Id: '7', Code: 'InternalError', Message: 'Internal error', SenderFault: false }
    // ...up to 10 entries
  ]
}
```

### Disabling the log

If your service already handles the `failed` entries and you don't want duplicated logs, set this env var in the function:

```sh
SQS_EMITTER_DISABLE_FAILED_LOG=true
```

> Only the exact value `'true'` disables it: any other value (or no value) keeps the log enabled.

`publishEvent()` doesn't log: it rejects, so the error is always visible to the caller.
