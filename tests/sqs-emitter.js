'use strict';

require('lllog')('none');

// lllog's factory returns a new Logger instance per call sharing the same prototype, so stubbing
// the prototype intercepts the logger.error() call made inside the module under test.
const LllogLogger = require('lllog')().constructor;

const sinon = require('sinon');
const assert = require('assert');

const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const { RAMClient, ListResourcesCommand } = require('@aws-sdk/client-ram');
const { SQSClient, SendMessageBatchCommand, SendMessageCommand } = require('@aws-sdk/client-sqs');

const { SqsEmitter, SqsEmitterError } = require('../lib');
const ParameterStore = require('../lib/helpers/parameter-store');
const S3Uploader = require('../lib/helpers/s3-uploader');

describe('SqsEmitter', () => {

	let sqsMock;
	let ssmMock;
	let ramMock;
	let s3Mock;
	let clock;

	const fakeDate = new Date(2025, 2, 6);
	const randomId = 'fake-id';
	const parameterName = 'shared/internal-storage';
	const sqsName = 'MySQSName';
	const sampleSqsUrl = `https://sqs.us-east-1.amazonaws.com/123456789012/${sqsName}`;
	const sampleSqsUrlFifo = `${sampleSqsUrl}.fifo`;
	const parameterNameStoreArn = `arn:aws:ssm:us-east-1:123456789012:parameter/${parameterName}`;
	const contentS3Path = `sqsContent/defaultClient/service-name/MySQSName/2025/03/06/${randomId}.json`;

	const buckets = [
		{
			bucketName: 'sample-bucket-name-us-east-1',
			region: 'us-east-1',
			default: true
		},
		{
			bucketName: 'sample-bucket-name-us-west-1',
			region: 'us-west-1'
		}
	];

	const assertRamListResourceCommand = () => {
		assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand, {
			resourceOwner: 'OTHER-ACCOUNTS'
		}, true).length, 1);
	};

	const assertSsmGetParameterCommand = () => {
		assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand, {
			Name: parameterNameStoreArn,
			WithDecryption: true
		}, true).length, 1);
	};

	const assertS3PutObjectCommand = (body, bucketName = buckets[0].bucketName, key = contentS3Path) => {
		assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand, {
			Bucket: bucketName,
			Key: key,
			Body: JSON.stringify(body)
		}, true).length, 1);
	};

	beforeEach(() => {
		ssmMock = mockClient(SSMClient);
		sqsMock = mockClient(SQSClient);
		ramMock = mockClient(RAMClient);
		s3Mock = mockClient(S3Client);
		clock = sinon.useFakeTimers(fakeDate.getTime());

		this.sqsEmitter = new SqsEmitter();
		this.sqsEmitter.session = { clientCode: 'defaultClient' };
		sinon.stub(this.sqsEmitter, 'randomId').get(() => randomId);

		process.env.JANIS_SERVICE_NAME = 'service-name';
	});

	afterEach(() => {
		sqsMock.restore();
		ssmMock.restore();
		ramMock.restore();
		s3Mock.restore();
		clock.restore();
		sinon.restore();
		ParameterStore.clearCache();
	});

	describe('AWS clients reuse', () => {

		it('Should share the SQS client between instances', () => {
			assert.strictEqual(new SqsEmitter().sqs, new SqsEmitter().sqs);
		});

		it('Should reuse the S3 client for uploads to the same region', async () => {

			s3Mock.on(PutObjectCommand).resolves({ ETag: 'etag' });

			await S3Uploader.uploadToBucket(buckets[0], contentS3Path, 'body');
			await S3Uploader.uploadToBucket(buckets[0], contentS3Path, 'body');

			const [firstCall, secondCall] = s3Mock.calls();

			assert.strictEqual(firstCall.thisValue, secondCall.thisValue);
		});

		it('Should use a different S3 client for each region', async () => {

			s3Mock.on(PutObjectCommand).resolves({ ETag: 'etag' });

			await S3Uploader.uploadToBucket(buckets[0], contentS3Path, 'body');
			await S3Uploader.uploadToBucket(buckets[1], contentS3Path, 'body');

			const [firstCall, secondCall] = s3Mock.calls();

			assert.notStrictEqual(firstCall.thisValue, secondCall.thisValue);
			assert.strictEqual(await firstCall.thisValue.config.region(), buckets[0].region);
			assert.strictEqual(await secondCall.thisValue.config.region(), buckets[1].region);
		});

		it('Should reuse the SSM and RAM clients between parameter lookups', async () => {

			ramMock.on(ListResourcesCommand).resolves({ resources: [{ arn: parameterNameStoreArn }] });
			ssmMock.on(GetParameterCommand).resolves({ Parameter: { Value: JSON.stringify(buckets) } });

			await ParameterStore.getParameterValue();
			ParameterStore.clearCache();
			await ParameterStore.getParameterValue();

			const [firstSsmCall, secondSsmCall] = ssmMock.calls();
			const [firstRamCall, secondRamCall] = ramMock.calls();

			assert.strictEqual(firstSsmCall.thisValue, secondSsmCall.thisValue);
			assert.strictEqual(firstRamCall.thisValue, secondRamCall.thisValue);
		});
	});

	describe('publishEvent', () => {

		const singleEventResponse = {
			messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
		};

		const singleEventFifoResponse = {
			messageId: '4ac0a219-1122-33b3-4445-5556666d734d',
			sequenceNumber: '222222222222222222222222'
		};

		it('Should publish a single event with content only as minimal requirement (Standard SQS)', async () => {

			sqsMock.on(SendMessageCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const result = await this.sqsEmitter.publishEvent(sampleSqsUrl, {
				content: { foo: 'bar' }
			});

			assert.deepStrictEqual(result, singleEventResponse);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand, {
				QueueUrl: sampleSqsUrl,
				MessageBody: JSON.stringify({ foo: 'bar' }),
				MessageAttributes: {
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					sqsName: {
						DataType: 'String',
						StringValue: sqsName
					}
				}
			}, true).length, 1);
		});

		it('Should publish a single event with s3 content path if it is greater than 256KB (FIFO SQS)', async () => {

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand).resolves({
				ETag: '5d41402abc4b2a76b9719d911017c590'
			});

			sqsMock.on(SendMessageCommand).resolves({
				MessageId: singleEventFifoResponse.messageId,
				SequenceNumber: singleEventFifoResponse.sequenceNumber
			});

			const result = await this.sqsEmitter.publishEvent(sampleSqsUrlFifo, {
				content: {
					bar: 'bar',
					foo: 'x'.repeat(256 * 1024)
				},
				payloadFixedProperties: ['bar']
			});

			assert.deepStrictEqual(result, singleEventFifoResponse);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand).length, 1);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();
			assertS3PutObjectCommand({
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			});

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand, {
				QueueUrl: sampleSqsUrlFifo,
				MessageBody: JSON.stringify({
					contentS3Location: {
						path: contentS3Path,
						bucketName: buckets[0].bucketName,
						region: buckets[0].region
					},
					bar: 'bar'
				}),
				MessageAttributes: {
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					sqsName: {
						DataType: 'String',
						StringValue: sqsName
					}
				}
			}, true).length, 1);
		});

		it('Should publish a single event with content only as minimal requirement (FIFO SQS)', async () => {

			sqsMock.on(SendMessageCommand).resolves({
				MessageId: singleEventFifoResponse.messageId,
				SequenceNumber: singleEventFifoResponse.sequenceNumber
			});

			const result = await this.sqsEmitter.publishEvent(sampleSqsUrlFifo, {
				content: { foo: 'bar' }
			});

			assert.deepStrictEqual(result, singleEventFifoResponse);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand, {
				QueueUrl: sampleSqsUrlFifo,
				MessageBody: JSON.stringify({ foo: 'bar' }),
				MessageAttributes: {
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					sqsName: {
						DataType: 'String',
						StringValue: sqsName
					}
				}
			}, true).length, 1);
		});

		it('Should publish a single event with all available properties (Standard SQS)', async () => {

			sqsMock.on(SendMessageCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const result = await this.sqsEmitter.publishEvent(sampleSqsUrl, {
				content: { foo: 'bar' },
				attributes: { foo: 'bar' },
				subject: 'test'
			});

			assert.deepEqual(sqsMock.commandCalls(SendMessageCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand, {
				QueueUrl: sampleSqsUrl,
				MessageBody: JSON.stringify({
					foo: 'bar'
				}),
				MessageAttributes: {
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					sqsName: {
						DataType: 'String',
						StringValue: sqsName
					},
					foo: {
						DataType: 'String',
						StringValue: 'bar'
					}
				},
				Subject: 'test'
			}, true).length, 1);

			assert.deepStrictEqual(result, singleEventResponse);
		});

		it('Should publish a single event with all available properties (FIFO SQS)', async () => {

			sqsMock.on(SendMessageCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const result = await this.sqsEmitter.publishEvent(sampleSqsUrlFifo, {
				content: { foo: 'bar' },
				attributes: { foo: 'bar', bar: true, mobile: ['foo', 'bar'] },
				subject: 'test',
				messageGroupId: 'group1',
				messageDeduplicationId: 'dedup1',
				messageStructure: 'json'
			});

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand, {
				QueueUrl: sampleSqsUrlFifo,
				MessageBody: JSON.stringify({ foo: 'bar' }),
				MessageAttributes: {
					'janis-client': {
						DataType: 'String',
						StringValue: 'defaultClient'
					},
					sqsName: {
						DataType: 'String',
						StringValue: sqsName
					},
					foo: {
						DataType: 'String',
						StringValue: 'bar'
					},
					bar: {
						DataType: 'String',
						StringValue: 'true'
					},
					mobile: {
						DataType: 'String',
						StringValue: JSON.stringify(['foo', 'bar'])
					}
				},
				Subject: 'test',
				MessageGroupId: 'group1',
				MessageDeduplicationId: 'dedup1',
				MessageStructure: 'json'
			}, true).length, 1);

			assert.deepStrictEqual(result, singleEventResponse);
		});

		it('Should emit event with s3 content path if it is greater than 256KB and the session is missing', async () => {

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				success: [
					{
						Id: '1',
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				],
				failed: []
			};

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand).resolves({
				ETag: '5d41402abc4b2a76b9719d911017c590'
			});

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [
					{ Id: '1', MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
				]
			});

			const content = {
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			};

			const customS3ContentPath = `sqsContent/core/service-name/MySQSName/2025/03/06/${randomId}.json`;

			this.sqsEmitter.session = null;

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content
				}
			]);

			assert.deepStrictEqual(result, partiallySentResponse);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 1);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();
			assertS3PutObjectCommand(content, buckets[0].bucketName, customS3ContentPath);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: [
					{
						Id: '1',
						MessageBody: JSON.stringify({
							contentS3Location: {
								path: customS3ContentPath,
								bucketName: buckets[0].bucketName,
								region: buckets[0].region
							},
							bar: 'bar'
						}),
						MessageAttributes: {
							sqsName: {
								DataType: 'String',
								StringValue: 'MySQSName'
							}
						}
					}]
			}, true).length, 1);

		});

		it('Should reject with S3_ERROR and not call SQS if a payload greater than 256KB cannot be uploaded to any S3 bucket', async () => {

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand).rejects(new Error('Error fetching S3'));

			sqsMock.on(SendMessageCommand);

			await assert.rejects(() => this.sqsEmitter.publishEvent(sampleSqsUrl, {
				payloadFixedProperties: ['bar'],
				content: {
					bar: 'bar',
					foo: 'x'.repeat(256 * 1024)
				}
			}), {
				name: 'SqsEmitterError',
				message: 'Failed to upload to all provided s3 buckets',
				code: SqsEmitterError.codes.S3_ERROR
			});

			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 2);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand).length, 0);
		});

		it('Should use s3 content path if the event is lower than 256KB in characters but greater in bytes', async () => {

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand).resolves({
				ETag: '5d41402abc4b2a76b9719d911017c590'
			});

			sqsMock.on(SendMessageCommand).resolves({
				MessageId: '4ac0a219-1122-33b3-4445-5556666d734d'
			});

			// 150K characters (< 256KB) but 300KB in UTF-8
			const content = { foo: 'ñ'.repeat(150 * 1024) };

			const result = await this.sqsEmitter.publishEvent(sampleSqsUrl, { content });

			assert.deepStrictEqual(result, { messageId: '4ac0a219-1122-33b3-4445-5556666d734d' });
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);
			assertS3PutObjectCommand(content);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand)[0].args[0].input.MessageBody, JSON.stringify({
				contentS3Location: {
					path: contentS3Path,
					bucketName: buckets[0].bucketName,
					region: buckets[0].region
				}
			}));
		});
	});

	describe('publishEvents', () => {

		afterEach(() => {
			sinon.restore();
			ParameterStore.clearCache();
		});

		const messageId = '4ac0a219-1122-33b3-4445-5556666d734d';

		const eventResponse = {
			successCount: 1,
			failedCount: 0,
			success: [{ Id: '1', messageId }],
			failed: []
		};

		const multiEventResponse = {
			successCount: 1,
			failedCount: 1,
			success: [
				{ Id: '1', messageId }
			],
			failed: [
				{
					Id: '2',
					Code: 'SQS001',
					Message: 'SQS Failed'
				}
			]
		};

		const multiEventFifoResponse = {
			successCount: 1,
			failedCount: 1,
			success: [
				{
					Id: '1',
					messageId: '4ac0a219-1122-33b3-4445-5556666d734d',
					sequenceNumber: '222222222222222222222222'
				}
			],
			failed: [
				{
					Id: '2',
					Code: 'SQS001',
					Message: 'SQS Failed'
				}
			]
		};

		it('Should publish multiple events with content only as minimal requirement (Standard SQS)', async () => {

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [
					{ Id: '1', MessageId: messageId }
				]
			});

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					content: { foo: 'bar' }
				}
			]);

			assert.deepStrictEqual(result, eventResponse);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 1);

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				Entries: [
					{
						Id: '1',
						MessageBody: JSON.stringify({ foo: 'bar' }),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							sqsName: {
								DataType: 'String',
								StringValue: sqsName
							}
						}
					}
				],
				QueueUrl: sampleSqsUrl
			}, true).length, 1);

		});

		it('Should publish multiple events with content only as minimal requirement (FIFO SQS)', async () => {

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [
					{
						Id: '1',
						MessageId: '4ac0a219-1122-33b3-4445-5556666d734d',
						SequenceNumber: '222222222222222222222222'
					}
				],
				Failed: [
					{
						Id: '2',
						Code: 'SQS001',
						Message: 'SQS Failed'
					}
				]
			});

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrlFifo, [
				{
					content: { foo: 'bar' }
				},
				{
					content: { foo: 'baz' }
				}
			]);

			assert.deepStrictEqual(result, multiEventFifoResponse);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrlFifo,
				Entries: [
					{
						Id: '1',
						MessageBody: JSON.stringify({ foo: 'bar' }),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							sqsName: {
								DataType: 'String',
								StringValue: sqsName
							}
						}
					},
					{
						Id: '2',
						MessageBody: JSON.stringify({ foo: 'baz' }),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							sqsName: {
								DataType: 'String',
								StringValue: sqsName
							}
						}
					}
				]
			}, true).length, 1);
		});

		it('Should reject if fails retrieve parameter name from ram resources', async () => {

			s3Mock.on(PutObjectCommand);
			sqsMock.on(SendMessageBatchCommand);
			ssmMock.on(GetParameterCommand);
			ramMock.on(ListResourcesCommand).rejects(new Error('RAM Internal Error'));

			const result = await assert.rejects(this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: 'Resource Access Manager Error: RAM Internal Error' });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);
			assertRamListResourceCommand();
		});

		it('Should reject if fails retrieve parameter from ssm parameter store', async () => {

			s3Mock.on(PutObjectCommand);
			sqsMock.on(SendMessageBatchCommand);

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).rejects(new Error('SSM Internal Error'));

			const result = await assert.rejects(this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: `Unable to get parameter with arn ${parameterNameStoreArn} - SSM Internal Error` });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);
			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();

		});

		it('Should not reject if cannot upload a payload to provided S3 buckets', async () => {

			sqsMock.on(SendMessageBatchCommand);

			s3Mock.on(PutObjectCommand)
				.rejectsOnce(new Error('Error fetching S3'))
				.rejectsOnce(new Error('Error fetching S3'));

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			const expectedResult = {
				successCount: 0,
				failedCount: 1,
				success: [],
				failed: [
					{
						Id: '1',
						Code: 'S3_ERROR',
						Message: 'Failed to upload to all provided s3 buckets'
					}
				]
			};

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]);

			assert.deepStrictEqual(result, expectedResult);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 2);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);
		});

		it('Should handle S3 upload failure for multiple payloads and return failure result without rejecting', async () => {

			// Use to restore the stubbed randomId
			sinon.restore();

			sqsMock
				.on(SendMessageBatchCommand)
				.resolvesOnce({
					Failed: [
						{ Id: '1', Code: 'SQS001', Message: 'SQS Failed' }
					]
				})
				.resolvesOnce({
					Successful: [
						{ Id: '2', MessageId: 'msg-2' }
					]
				});

			s3Mock
				.on(PutObjectCommand)
				.rejectsOnce(new Error('Error fetching S3'))
				.rejectsOnce(new Error('Error fetching S3'))
				.resolvesOnce({ ETag: '5d41402abc4b2a76b9719d911017c591' });

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			const expectedResult = {
				successCount: 0,
				failedCount: 2,
				success: [],
				failed: [
					{ Id: '1', Code: 'SQS001', Message: 'SQS Failed' },
					{
						Id: '3',
						Message: 'Failed to upload to all provided s3 buckets',
						Code: 'S3_ERROR'
					}
				]
			};

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['foo'],
					content: {
						foo: 'bar',
						bar: 'bar'
					}
				},
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				},
				{
					payloadFixedProperties: ['foo'],
					content: {
						foo: 'foo',
						bar: 'x'.repeat(256 * 1024)
					}
				}
			]);

			assert.deepStrictEqual(result, expectedResult);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 4);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 1);
		});

		it('Should reject if the queue url format is not valid', async () => {

			s3Mock.on(PutObjectCommand);
			sqsMock.on(SendMessageBatchCommand);
			ssmMock.on(GetParameterCommand);
			ramMock.on(ListResourcesCommand);

			const invalidSqsUrl = 'https://invalid-url';

			const result = await assert.rejects(this.sqsEmitter.publishEvents(invalidSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: `Invalid SQS URL: ${invalidSqsUrl}` });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 0);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);

		});

		it('Should reject if cannot find resources with the parameter name in the ARN', async () => {

			s3Mock.on(PutObjectCommand);
			sqsMock.on(SendMessageBatchCommand);
			ssmMock.on(GetParameterCommand);

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: 'other-arn-without-the-parameter-name' }]
			});

			const result = await assert.rejects(this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: `Resource Access Manager Error: Unable to find resources with parameter /${parameterName} in the ARN` });

			assert.deepEqual(result, undefined);
			assert.deepEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);
			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assertRamListResourceCommand();

		});

		it('Should upload a payload to some provisional S3 bucket if the default bucket upload fails', async () => {

			const content = {
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			};

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				success: [
					{
						Id: '1',
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				],
				failed: []
			};

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand)
				.rejectsOnce(new Error('Error fetching S3'))
				.resolvesOnce({
					ETag: '5d41402abc4b2a76b9719d911017c590'
				});

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [
					{ Id: '1', MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
				]
			});

			this.sqsEmitter.session = { clientCode: 'defaultClient' };

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					content,
					payloadFixedProperties: ['bar']
				}
			]);

			assert.deepStrictEqual(result, partiallySentResponse);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 2);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 1);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();
			assertS3PutObjectCommand(content, buckets[0].bucketName);
			assertS3PutObjectCommand(content, buckets[1].bucketName);

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: [
					{
						Id: '1',
						MessageBody: JSON.stringify({
							contentS3Location: {
								path: contentS3Path,
								bucketName: buckets[1].bucketName,
								region: buckets[1].region
							},
							bar: 'bar'
						}),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							sqsName: {
								DataType: 'String',
								StringValue: 'MySQSName'
							}
						}
					}]
			}, true).length, 1);

		});

		it('Should emit event with s3 content path if it is greater than 256KB', async () => {

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				success: [
					{
						Id: '1',
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				],
				failed: []
			};

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			s3Mock.on(PutObjectCommand).resolves({
				ETag: '5d41402abc4b2a76b9719d911017c590'
			});

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [
					{ Id: '1', MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
				]
			});

			const content = {
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			};

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content
				}
			]);

			assert.deepStrictEqual(result, partiallySentResponse);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 1);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();
			assertS3PutObjectCommand(content);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: [
					{
						Id: '1',
						MessageBody: JSON.stringify({
							contentS3Location: {
								path: contentS3Path,
								bucketName: buckets[0].bucketName,
								region: buckets[0].region
							},
							bar: 'bar'
						}),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							sqsName: {
								DataType: 'String',
								StringValue: 'MySQSName'
							}
						}
					}]
			}, true).length, 1);

		});

		it('Should split events in batches not greater than 256KB', async () => {

			sqsMock.on(SendMessageBatchCommand)
				.resolvesOnce({
					Successful: [
						{ Id: '1', MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
					]
				})
				.resolvesOnce({
					Failed: [{
						Id: '2',
						Code: 'SQS001',
						Message: 'SQS Failed'
					}]
				});

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					content: {
						foo: 'x'.repeat(150 * 1024)
					}
				},
				{
					content: {
						foo: 'y'.repeat(150 * 1024)
					}
				}
			]);

			assert.deepStrictEqual(result, multiEventResponse);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 0);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 2);

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: [
					{
						Id: '1',
						MessageBody: JSON.stringify({
							foo: 'x'.repeat(150 * 1024)
						}),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							sqsName: {
								DataType: 'String',
								StringValue: 'MySQSName'
							}
						}
					}
				]
			}, true).length, 1);

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: [
					{
						Id: '2',
						MessageBody: JSON.stringify({
							foo: 'y'.repeat(150 * 1024)
						}),
						MessageAttributes: {
							'janis-client': {
								DataType: 'String',
								StringValue: 'defaultClient'
							},
							sqsName: {
								DataType: 'String',
								StringValue: 'MySQSName'
							}
						}

					}
				]
			}, true).length, 1);
		});

		it('Should split events in different batches if their total size is greater than 256KB in bytes but lower in characters', async () => {

			sqsMock.on(SendMessageBatchCommand).resolves({});

			// 70K characters each (140K in total) but 140KB each in UTF-8
			await this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{ content: { foo: 'ñ'.repeat(70 * 1024) } },
				{ content: { foo: 'ñ'.repeat(70 * 1024) } }
			]);

			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 2);
		});

		it('Should split events in batches not greater than 10 entries', async () => {

			sqsMock.on(SendMessageBatchCommand)
				.resolvesOnce({
					Successful: [
						{ Id: '1', MessageId: 'msg-1' },
						{ Id: '2', MessageId: 'msg-2' }
					]
				})
				.resolvesOnce({
					Failed: [{
						Id: '3',
						Code: 'SQS001',
						Message: 'SQS Failed'
					}]
				});

			// should have 2 batches, first with 10 and the second with 5
			const events = Array.from({ length: 15 }, (_, index) => ({
				content: { message: `Event ${index + 1}` }
			}));

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, events);

			assert.deepStrictEqual(result.successCount, 2);
			assert.deepStrictEqual(result.failedCount, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 2);

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: Array.from({ length: 10 }, (_, index) => ({
					Id: `${index + 1}`,
					MessageBody: JSON.stringify({ message: `Event ${index + 1}` }),
					MessageAttributes: {
						sqsName: {
							DataType: 'String',
							StringValue: 'MySQSName'
						},
						'janis-client': {
							DataType: 'String',
							StringValue: 'defaultClient'
						}
					}

				}))
			}, true).length, 1);

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: Array.from({ length: 5 }, (_, index) => ({
					Id: `${index + 11}`,
					MessageBody: JSON.stringify({ message: `Event ${index + 11}` }),
					MessageAttributes: {
						'janis-client': {
							DataType: 'String',
							StringValue: 'defaultClient'
						},
						sqsName: {
							DataType: 'String',
							StringValue: 'MySQSName'
						}
					}
				}))
			}, true).length, 1);

		});

	});

	describe('Failed events log', () => {

		let loggerErrorStub;
		const originalDisableFailedLog = process.env.SQS_EMITTER_DISABLE_FAILED_LOG;

		beforeEach(() => {
			loggerErrorStub = sinon.stub(LllogLogger.prototype, 'error');
			delete process.env.SQS_EMITTER_DISABLE_FAILED_LOG;
		});

		afterEach(() => {
			if(originalDisableFailedLog === undefined)
				delete process.env.SQS_EMITTER_DISABLE_FAILED_LOG;
			else
				process.env.SQS_EMITTER_DISABLE_FAILED_LOG = originalDisableFailedLog;
		});

		const buildEvents = length => Array.from({ length }, (_, index) => ({
			content: { secret: `personal-data-${index + 1}` },
			attributes: { secretAttribute: 'personal-attribute' }
		}));

		it('Should log once the failed entries without the message content', async () => {

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [{ Id: '1', MessageId: 'message-1' }],
				Failed: [{
					Id: '2',
					SenderFault: true,
					Code: 'SQS001',
					Message: 'SQS Failed',
					MessageBody: 'should-not-be-logged'
				}]
			});

			await this.sqsEmitter.publishEvents(sampleSqsUrl, buildEvents(2));

			sinon.assert.calledOnce(loggerErrorStub);
			sinon.assert.calledWithExactly(loggerErrorStub, 'Failed to publish SQS events', {
				sqsName: 'MySQSName',
				clientCode: 'defaultClient',
				failedCount: 1,
				totalCount: 2,
				failed: [{ Id: '2', Code: 'SQS001', Message: 'SQS Failed', SenderFault: true }]
			});

			assert.ok(!JSON.stringify(loggerErrorStub.args).match(/personal-|should-not-be-logged/));
		});

		it('Should log without clientCode and SenderFault when they do not exist', async () => {

			this.sqsEmitter.session = undefined;

			sqsMock.on(SendMessageBatchCommand).resolves({
				Failed: [{ Id: '1', Code: 'SQS001', Message: 'SQS Failed' }]
			});

			await this.sqsEmitter.publishEvents(sampleSqsUrl, buildEvents(1));

			sinon.assert.calledOnceWithExactly(loggerErrorStub, 'Failed to publish SQS events', {
				sqsName: 'MySQSName',
				failedCount: 1,
				totalCount: 1,
				failed: [{ Id: '1', Code: 'SQS001', Message: 'SQS Failed' }]
			});
		});

		it('Should log only the first 10 failed entries with the total count', async () => {

			sqsMock.on(SendMessageBatchCommand).callsFake(({ Entries }) => ({
				Failed: Entries.map(({ Id }) => ({ Id, Code: 'SQS001', Message: 'SQS Failed' }))
			}));

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, buildEvents(25));

			assert.deepStrictEqual(result.failedCount, 25);

			sinon.assert.calledOnce(loggerErrorStub);

			const [, metadata] = loggerErrorStub.firstCall.args;

			assert.deepStrictEqual(metadata.failedCount, 25);
			assert.deepStrictEqual(metadata.totalCount, 25);
			assert.deepStrictEqual(metadata.failed.map(({ Id }) => Id), Array.from({ length: 10 }, (_, index) => `${index + 1}`));
		});

		it('Should not log when there are no failed entries', async () => {

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [{ Id: '1', MessageId: 'message-1' }]
			});

			await this.sqsEmitter.publishEvents(sampleSqsUrl, buildEvents(1));

			sinon.assert.notCalled(loggerErrorStub);
		});

		it('Should not log when SQS_EMITTER_DISABLE_FAILED_LOG is true', async () => {

			process.env.SQS_EMITTER_DISABLE_FAILED_LOG = 'true';

			sqsMock.on(SendMessageBatchCommand).resolves({
				Failed: [{ Id: '1', Code: 'SQS001', Message: 'SQS Failed' }]
			});

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, buildEvents(1));

			assert.deepStrictEqual(result.failedCount, 1);
			sinon.assert.notCalled(loggerErrorStub);
		});

		it('Should log when SQS_EMITTER_DISABLE_FAILED_LOG has a value other than true', async () => {

			process.env.SQS_EMITTER_DISABLE_FAILED_LOG = 'false';

			sqsMock.on(SendMessageBatchCommand).resolves({
				Failed: [{ Id: '1', Code: 'SQS001', Message: 'SQS Failed' }]
			});

			await this.sqsEmitter.publishEvents(sampleSqsUrl, buildEvents(1));

			sinon.assert.calledOnce(loggerErrorStub);
		});
	});
});
