'use strict';

require('lllog')('none');

const sinon = require('sinon');
const assert = require('assert');

const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const { RAMClient, ListResourcesCommand } = require('@aws-sdk/client-ram');
const { STSClient, AssumeRoleCommand } = require('@aws-sdk/client-sts');

const { SQSClient, SendMessageBatchCommand } = require('@aws-sdk/client-sqs');

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
	const sampleSqsUrl = 'arn:aws:sns:us-east-1:123456789012:MySQSName';
	const parameterNameStoreArn = `arn:aws:ssm:us-east-1:12345678:parameter/${parameterName}`;
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

	const getSqsEntry = (id, content, extraAttributes) => ({
		Id: String(id),
		MessageBody: JSON.stringify(content),
		MessageAttributes: {
			...extraAttributes,
			sqsName: {
				DataType: 'String',
				StringValue: 'MySQSName'
			}
		}
	});

	const assertListResourceCommand = () => {
		assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand, {
			resourceOwner: 'OTHER-ACCOUNTS'
		}, true).length, 1);
	};

	const assertGetParameterCommand = () => {
		assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand, {
			Name: parameterNameStoreArn,
			WithDecryption: true
		}, true).length, 1);
	};

	const assertAssumeRoleCommand = (callsNumber = 1) => {
		assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand, {
			RoleArn: buckets[0].roleArn,
			RoleSessionName: 'service-name',
			DurationSeconds: 1800
		}, true).length, callsNumber);
	};

	const assertPutObjectCommand = (body, bucketName = buckets[0].bucketName) => {
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
		process.env.JANIS_SERVICE_NAME = 'service-name';

	});

	afterEach(() => {
		sqsMock.restore();
		ssmMock.restore();
		ramMock.restore();
		s3Mock.restore();
		stsMock.restore();
		clock.restore();
	});

	describe('publishEvents', () => {

		beforeEach(() => {
			this.sqsEmiter = new SqsEmitter();
			sinon.stub(this.sqsEmiter, 'randomId').get(() => randomId);
		});

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

			const result = await this.sqsEmiter.publishEvents(sampleSqsUrl, [
				{
					content: { foo: 'bar' }
				}
			]);

			assert.deepStrictEqual(result, eventResponse);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 1);

			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand, {
				Entries: [getSqsEntry(1, { foo: 'bar' })],
				QueueUrl: sampleSqsUrl
			}, true).length, 1);

		});

		it('Should publish multiple events with content only as minimal requirement (FIFO Topic)', async () => {

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

			const result = await this.sqsEmiter.publishEvents(sampleSqsUrl, [
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
					getSqsEntry(1, { foo: 'bar' }),
					getSqsEntry(2, { foo: 'baz' })
				]
			}, true).length, 1);
		});

		it('Should skip if fails if cannot get parameter from parametter store', async () => {

			stsMock.on(AssumeRoleCommand);
			s3Mock.on(PutObjectCommand);
			sqsMock.on(SendMessageBatchCommand);

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: parameterNameStoreArn }]
			});

			ssmMock.on(GetParameterCommand).rejects(new Error('SSM Internal Error'));

			const sqsTrigger = new SqsEmitter();

			sqsTrigger.session = { clientCode: 'defaultClient' };

			const result = await assert.rejects(sqsTrigger.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]));

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);

			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand, {
				resourceOwner: 'OTHER-ACCOUNTS'
			}, true).length, 1);

			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand, {
				Name: parameterNameStoreArn,
				WithDecryption: true
			}, true).length, 1);

			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);

		});

		it('Should skip event if cannot get resources with the parameter name in the ARN', async () => {

			stsMock.on(AssumeRoleCommand);
			s3Mock.on(PutObjectCommand);
			sqsMock.on(SendMessageBatchCommand);
			ssmMock.on(GetParameterCommand);

			ramMock.on(ListResourcesCommand).resolves({
				resources: [{ arn: 'other-arn-without-the-parameter-name' }]
			});

			const sqsTrigger = new SqsEmitter();

			sqsTrigger.session = { clientCode: 'defaultClient' };

			const result = await assert.rejects(sqsTrigger.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]));

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 0);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 0);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);

			assertListResourceCommand();

			assert.deepEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);

		});

		it('Should skip events if fail to retrieve credentials to assume role', async () => {

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

			const sqsTrigger = new SqsEmitter();

			sqsTrigger.session = { clientCode: 'defaultClient' };

			const result = await assert.rejects(sqsTrigger.publishEvents(sampleSqsUrl, [
				{
					payloadFixedProperties: ['bar'],
					content: {
						bar: 'bar',
						foo: 'x'.repeat(256 * 1024)
					}
				}
			]));

			assert.deepStrictEqual(result, undefined);
			assert.deepStrictEqual(ramMock.commandCalls(ListResourcesCommand).length, 1);
			assert.deepStrictEqual(ssmMock.commandCalls(GetParameterCommand).length, 1);
			assert.deepStrictEqual(stsMock.commandCalls(AssumeRoleCommand).length, 2);
			assert.deepStrictEqual(s3Mock.commandCalls(PutObjectCommand).length, 0);
			assert.deepStrictEqual(sqsMock.commandCalls(SendMessageBatchCommand).length, 0);

			assertListResourceCommand();

			assertGetParameterCommand();

			assertAssumeRoleCommand(2);

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

			this.sqsEmiter.session = { clientCode: 'defaultClient' };

			const result = await this.sqsEmiter.publishEvents(sampleSqsUrl, [
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

			assertListResourceCommand();
			assertGetParameterCommand();
			assertAssumeRoleCommand(2);
			assertPutObjectCommand(content, buckets[0].bucketName);
			assertPutObjectCommand(content, buckets[1].bucketName);

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

		it('Should save the event content in S3 if it is greater than 256KB', async () => {

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

			this.sqsEmiter.session = { clientCode: 'defaultClient' };

			const content = {
				bar: 'bar',
				foo: 'x'.repeat(256 * 1024)
			};

			const result = await this.sqsEmiter.publishEvents(sampleSqsUrl, [
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

			assertListResourceCommand();
			assertGetParameterCommand();
			assertAssumeRoleCommand();
			assertPutObjectCommand(content);

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

			const result = await this.sqsEmiter.publishEvents(sampleSqsUrl, [
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

			const sqsTrigger = new SqsEmitter();
			const result = await sqsTrigger.publishEvents(sampleSqsUrl, events);

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
