'use strict';

require('lllog')('none');

const sinon = require('sinon');
const assert = require('assert');

const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const { RAMClient, ListResourcesCommand } = require('@aws-sdk/client-ram');
const { STSClient, AssumeRoleCommand } = require('@aws-sdk/client-sts');
const { SQSClient, SendMessageBatchCommand, SendMessageCommand } = require('@aws-sdk/client-sqs');

const {
	SqsEmitter
	// SqsEmitterError
} = require('../lib');
const ParameterStore = require('../lib/helpers/parameter-store');

describe('SqsEmitter', () => {

	let sqsMock;
	let ssmMock;
	let ramMock;
	let s3Mock;
	let stsMock;
	let clock;

	const fakeDate = new Date(2025, 2, 6);
	const randomId = 'fake-id';
	const parameterName = 'shared/internal-storage';
	const sqsName = 'MySQSName';
	const sampleSqsUrl = `https://sqs.us-east-1.amazonaws.com/123456789012/${sqsName}`;
	const sampleSqsUrlFifo = `${sampleSqsUrl}.fifo`;
	const parameterNameStoreArn = `arn:aws:ssm:us-east-1:123456789012:parameter/${parameterName}`;
	const s3ContentPath = `sqsContent/defaultClient/service-name/MySQSName/2025/03/06/${randomId}.json`;

	const credentials = {
		AccessKeyId: 'accessKeyIdTest',
		SecretAccessKey: 'secretAccessKeyTest',
		SessionToken: 'sessionTokenTest'
	};

	const buckets = [
		{
			bucketName: 'sample-bucket-name-us-east-1',
			roleArn: 'arn:aws:iam::1234567890:role/defaultRoleName',
			region: 'us-east-1',
			default: true
		},
		{
			bucketName: 'sample-bucket-name-us-west-1',
			roleArn: 'arn:aws:iam::1234567890:role/defaultRoleName',
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

	const assertStsAssumeRoleCommand = (callsNumber = 1) => {
		assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand, {
			RoleArn: buckets[0].roleArn,
			RoleSessionName: 'service-name',
			DurationSeconds: 1800
		}, true).length, callsNumber);
	};

	const assertS3PutObjectCommand = (body, bucketName = buckets[0].bucketName) => {
		assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand, {
			Bucket: bucketName,
			Key: s3ContentPath,
			Body: JSON.stringify(body)
		}, true).length, 1);
	};

	beforeEach(() => {
		ssmMock = mockClient(SSMClient);
		sqsMock = mockClient(SQSClient);
		ramMock = mockClient(RAMClient);
		s3Mock = mockClient(S3Client);
		stsMock = mockClient(STSClient);
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
		stsMock.restore();
		clock.restore();
		sinon.restore();
		ParameterStore.clearCache();
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
				Entries: [
					{
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
				]
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

			stsMock.on(AssumeRoleCommand).resolves({
				Credentials: credentials
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
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand, {
				QueueUrl: sampleSqsUrlFifo,
				Entries: [
					{
						MessageBody: JSON.stringify({ s3ContentPath, bar: 'bar' }),
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
				Entries: [
					{
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
				]
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

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: [
					{
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
					}
				]
			}, true).length, 1);

			assert.deepStrictEqual(result, singleEventResponse);
		});

		it('Should publish a single event with all available properties (FIFO SQS)', async () => {

			sqsMock.on(SendMessageCommand).resolves({
				MessageId: singleEventResponse.messageId
			});

			const result = await this.sqsEmitter.publishEvent(sampleSqsUrlFifo, {
				content: { foo: 'bar' },
				attributes: { foo: 'bar' },
				subject: 'test',
				messageGroupId: 'group1',
				messageDeduplicationId: 'dedup1',
				messageStructure: 'json'
			});

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageCommand, {
				QueueUrl: sampleSqsUrlFifo,
				Entries: [
					{
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
							}
						},
						Subject: 'test',
						MessageGroupId: 'group1',
						MessageDeduplicationId: 'dedup1',
						MessageStructure: 'json'
					}
				]
			}, true).length, 1);

			assert.deepStrictEqual(result, singleEventResponse);
		});

		it('Should fail if session with clientCode is missing', async () => {

			stsMock.on(AssumeRoleCommand);
			s3Mock.on(PutObjectCommand);
			sqsMock.on(SendMessageBatchCommand);
			ssmMock.on(GetParameterCommand);
			ramMock.on(ListResourcesCommand);

			this.sqsEmitter.session = {};

			const result = await assert.rejects(this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: 'The session must have a clientCode' });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 0);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);
			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);

			// assertRamListResourceCommand();
			// assertSsmGetParameterCommand();

		});

	});

	describe('publishEvents', () => {

		afterEach(() => {
			sinon.restore();
		});

		afterEach(() => {
			ParameterStore.clearCache();
		});

		const messageId = '4ac0a219-1122-33b3-4445-5556666d734d';

		const eventResponse = {
			successCount: 1,
			failedCount: 0,
			results: [
				{
					success: true,
					messageId
				}
			]
		};

		const multiEventResponse = {
			successCount: 1,
			failedCount: 1,
			results: [
				{
					success: true,
					messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
				},
				{
					success: false,
					errorCode: 'SQS001',
					errorMessage: 'SQS Failed'
				}
			]
		};

		const multiEventFifoResponse = {
			successCount: 1,
			failedCount: 1,
			results: [
				{
					success: true,
					messageId: '4ac0a219-1122-33b3-4445-5556666d734d',
					sequenceNumber: '222222222222222222222222'
				},
				{
					success: false,
					errorCode: 'SQS001',
					errorMessage: 'SQS Failed'
				}
			]
		};

		it('Should publish multiple events with content only as minimal requirement (Standard SQS)', async () => {

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [
					{ MessageId: messageId }
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
						MessageId: '4ac0a219-1122-33b3-4445-5556666d734d',
						SequenceNumber: '222222222222222222222222'
					}
				],
				Failed: [
					{
						Code: 'SQS001',
						Message: 'SQS Failed'
					}
				]
			});

			const result = await this.sqsEmitter.publishEvents(sampleSqsUrl, [
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
				QueueUrl: sampleSqsUrl,
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

			stsMock.on(AssumeRoleCommand);
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);
			assertRamListResourceCommand();
		});

		it('Should reject if fails retrieve parameter from ssm parameter store', async () => {

			stsMock.on(AssumeRoleCommand);
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);
			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();

		});

		it('Should fail if the que url format is not valid', async () => {

			stsMock.on(AssumeRoleCommand);
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);

		});

		it('Should fail if cannot find resources with the parameter name in the ARN', async () => {

			stsMock.on(AssumeRoleCommand);
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

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);

			assertRamListResourceCommand();

			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);

		});

		it('Should reject if fail to retrieve credentials to assume role', async () => {

			sqsMock.on(SendMessageBatchCommand);
			s3Mock.on(PutObjectCommand);

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			stsMock.on(AssumeRoleCommand)
				.rejects(new Error('Not authorized'));

			const result = await assert.rejects(this.sqsEmitter.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]), { message: 'Failed to upload to both default and provisional buckets' });

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 2);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();
			assertStsAssumeRoleCommand(2);
		});

		it('Should upload a payload to the provisional S3 bucket if the default bucket upload fails', async () => {

			const content = {
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			};

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				results: [
					{
						success: true,
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				]
			};

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			stsMock.on(AssumeRoleCommand).resolves({
				Credentials: credentials
			});

			s3Mock.on(PutObjectCommand)
				.rejectsOnce(new Error('Error fetching S3'))
				.resolvesOnce({
					ETag: '5d41402abc4b2a76b9719d911017c590'
				});

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [
					{ MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 2);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 2);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 1);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();
			assertStsAssumeRoleCommand(2);
			assertS3PutObjectCommand(content, buckets[0].bucketName);
			assertS3PutObjectCommand(content, buckets[1].bucketName);

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: [
					{
						Id: '1',
						MessageBody: JSON.stringify({ s3ContentPath, bar: 'bar' }),
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

		it('Should publish event with s3 content path if it is greater than 256KB', async () => {

			const partiallySentResponse = {
				successCount: 1,
				failedCount: 0,
				results: [
					{
						success: true,
						messageId: '4ac0a219-1122-33b3-4445-5556666d734d'
					}
				]
			};

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).resolves({
				Parameter: {
					Value: JSON.stringify(buckets)
				}
			});

			stsMock.on(AssumeRoleCommand).resolves({
				Credentials: credentials
			});

			s3Mock.on(PutObjectCommand).resolves({
				ETag: '5d41402abc4b2a76b9719d911017c590'
			});

			sqsMock.on(SendMessageBatchCommand).resolves({
				Successful: [
					{ MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 1);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 1);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 1);

			assertRamListResourceCommand();
			assertSsmGetParameterCommand();
			assertStsAssumeRoleCommand();
			assertS3PutObjectCommand(content);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				QueueUrl: sampleSqsUrl,
				Entries: [
					{
						Id: '1',
						MessageBody: JSON.stringify({ s3ContentPath, bar: 'bar' }),
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
						{ MessageId: '4ac0a219-1122-33b3-4445-5556666d734d' }
					]
				})
				.resolvesOnce({
					Failed: [{
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
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
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

		it('Should split events in batches not greater than 10 entries', async () => {

			sqsMock.on(SendMessageBatchCommand)
				.resolvesOnce({
					Successful: [
						{ MessageId: 'msg-1' },
						{ MessageId: 'msg-2' }
					]
				})
				.resolvesOnce({
					Failed: [{
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
});
