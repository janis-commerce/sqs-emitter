/* istanbul ignore file */

// 'use strict';

'use strict';

const logger = require('lllog')();
const { SQSClient, SendMessageBatchCommand, SendMessageCommand } = require('@aws-sdk/client-sqs');

const AsyncWithConcurrency = require('./helpers/async-with-concurrency');
const ParameterStore = require('./helpers/parameter-store');
const S3Uploader = require('./helpers/s3-uploader');
const { randomValue } = require('./helpers/id-helper');
const { pickProperties } = require('./helpers/pick-properties');
// const SqsEmitterError = require('./sqs-emitter-error');

const MAX_CONCURRENCY = 25;

// 256 KB Limit
const SQS_MESSAGE_LIMIT_SIZE = 256 * 1024;

// 10 messages per batch request
const SQS_MAX_BATCH_SIZE = 10;

module.exports = class SqsEmitter {

	constructor() {
		/** @private */
		this.sqs ??= new SQSClient();
	}

	/**
	 * @type {SQSClient}
	 * @private
	 */
	get sqs() {
		return this._sqs;
	}

	/**
	 * @private
	 */
	set sqs(sqsClient) {
		/** @private */
		this._sqs = sqsClient;
	}

	get randomId() {
		return randomValue(13);
	}

	publishEvent(sqsUrl, event) {

		const sqsName = this.getSqsNameFromArn(sqsUrl);

		const command = new SendMessageCommand({
			QueueUrl: sqsUrl,
			...this.formatSQSEvent(event, sqsName)
		});

		const sqsResponse = this.sqs.send(command);

		return {
			messageId: sqsResponse.MessageId,
			...sqsResponse.SequenceNumber && { sequenceNumber: sqsResponse.SequenceNumber }
		};
	}

	async publishEvents(sqsUrl, events) {

		const parsedEvents = this.parseEvents(events, sqsUrl);

		const asyncWithConcurrency = new AsyncWithConcurrency(async batch => {

			const promises = [];
			const formattedSqsBatch = [];

			for(const { s3ContentPath, payloadFixedProperties, ...data } of batch) {

				if(!s3ContentPath) {
					formattedSqsBatch.push(data);
					continue;
				}

				const bucketList = await ParameterStore.getParameterValue();

				if(!bucketList)
					continue;

				const contentFixedToSqs = {
					s3ContentPath,
					...payloadFixedProperties?.length && {
						...pickProperties(JSON.parse(data.MessageBody), payloadFixedProperties)
					}
				};

				formattedSqsBatch.push({ ...data, MessageBody: JSON.stringify(contentFixedToSqs) });

				promises.push(S3Uploader.uploadS3ContentPath(bucketList, s3ContentPath, data.MessageBody));
			}

			await Promise.all(promises);

			if(!formattedSqsBatch.length)
				return;

			return this.sqs.send(new SendMessageBatchCommand({
				Entries: formattedSqsBatch,
				QueueUrl: sqsUrl
			}));

		}, MAX_CONCURRENCY);

		/** @type {import('@aws-sdk/client-sqs').SendMessageBatchCommand[]} */
		const results = await asyncWithConcurrency.run(parsedEvents);

		return this.formatSQSResponse(results, parsedEvents);
	}

	formatSQSResponse(results) {

		const response = {
			successCount: 0,
			failedCount: 0,
			results: []
		};

		results.forEach(result => {

			if(result.Successful) {

				response.successCount += result.Successful.length;

				result.Successful.forEach(success => response.results.push({
					success: true,
					messageId: success.MessageId,
					...success.SequenceNumber && { sequenceNumber: success.SequenceNumber }
				}));

			}

			if(result.Failed) {

				response.failedCount += result.Failed.length;

				result.Failed.forEach(failed => response.results.push({
					success: false,
					errorCode: failed.Code,
					errorMessage: failed.Message
				}));

			}
		});

		return response;
		// return {
		// 	successCount,
		// 	failedCount,
		// 	results
		// };

		// return results.reduce((response, result, index) => {

		// 	const batchMessages = messages.slice(MAX_MESSAGES_PER_BATCH * index, MAX_MESSAGES_PER_BATCH * (index + 1));

		// 	// Full request fail. All the chunk must be retried.
		// 	if(result.status === 'rejected') {

		// 		response.failedCount += batchMessages.length;

		// 		response.results.push(...batchMessages.map(message => ({
		// 			message: JSON.parse(message.MessageBody),
		// 			error: {
		// 				message: result.reason?.message
		// 			}
		// 		})));

		// 		return response;
		// 	}

		// 	if(result.value.Successful?.length) {

		// 		response.successfulCount += result.value.Successful.length;

		// 		response.results.push(...result.value.Successful.map(({ Id, MessageId }) => {

		// 			const idResult = Id.replace('message-', '');

		// 			return {
		// 				message: JSON.parse(batchMessages[idResult].MessageBody),
		// 				messageId: MessageId
		// 			};
		// 		}));
		// 	}

		// 	if(result.value.Failed?.length) {

		// 		response.failedCount += result.value.Failed.length;

		// 		response.results.push(...result.value.Failed.map(({ Id, Message, Code, SenderFault }) => {

		// 			const message = Id.replace('message-', '');

		// 			return {
		// 				message: JSON.parse(batchMessages[message].MessageBody),
		// 				error: {
		// 					message: Message || Code,
		// 					code: Code,
		// 					isClientError: !!SenderFault
		// 				}
		// 			};
		// 		}));
		// 	}

		// 	return response;

		// }, initialState);
	}

	parseEvents(events, sqsUrl) {

		const parsedEvents = [
			[]
		];

		let currentBatchIndex = 0;
		let currentBatchSize = 0;

		let eventIndex = 0;

		const sqsName = this.getSqsNameFromArn(sqsUrl);

		for(const event of events) {

			eventIndex++;

			let parsedEvent = this.formatSQSEvent(event, sqsName, eventIndex);

			let parsedEventSize = JSON.stringify(parsedEvent).length;

			[parsedEvent, parsedEventSize] = this.handleEventSizeLimit(parsedEvent, parsedEventSize, sqsName);

			if(currentBatchSize + parsedEventSize > SQS_MESSAGE_LIMIT_SIZE || parsedEvents[currentBatchIndex].length === SQS_MAX_BATCH_SIZE) {
				currentBatchIndex++;
				parsedEvents[currentBatchIndex] = [];
				currentBatchSize = 0;
			}

			parsedEvents[currentBatchIndex].push(parsedEvent);
			currentBatchSize += parsedEventSize;
		}

		return parsedEvents;
	}

	formatSQSEvent(event, sqsName, eventIndex) {

		const parsedAttributes = this.parseMessageAttributes(event.attributes, sqsName);

		return {
			...eventIndex && { Id: `${eventIndex}` },
			MessageBody: JSON.stringify(event.content),
			MessageAttributes: parsedAttributes,
			...event.subject && { Subject: event.subject },
			...event.messageDeduplicationId && { MessageDeduplicationId: event.messageDeduplicationId },
			...event.messageGroupId && { MessageGroupId: event.messageGroupId },
			...event.messageStructure && { MessageStructure: event.messageStructure },
			...event.payloadFixedProperties && { payloadFixedProperties: event.payloadFixedProperties },
			...parsedAttributes && { MessageAttributes: parsedAttributes }
		};
	}

	handleEventSizeLimit(parsedEvent, parsedEventSize, sqsName) {

		if(parsedEventSize > SQS_MESSAGE_LIMIT_SIZE) {

			parsedEvent.s3ContentPath = this.getS3ContentPath(sqsName);

			const { payloadFixedProperties, s3ContentPath } = parsedEvent;

			const contentFixed = {
				s3ContentPath,
				...payloadFixedProperties?.length && {
					...pickProperties(JSON.parse(parsedEvent.MessageBody), payloadFixedProperties)
				}
			};

			parsedEventSize = JSON.stringify(contentFixed).length;

			logger.info('Parsed event size exceeds the 256KB limit. It will be sent with an S3 content path: ', parsedEvent.s3ContentPath);
		}

		return [parsedEvent, parsedEventSize];
	}

	getS3ContentPath(sqsName) {

		const now = new Date();
		const extension = 'json';

		return [
			'sqsContent',
			`${this.session.clientCode}`,
			`${process.env.JANIS_SERVICE_NAME}`,
			sqsName,
			this.formatDate(now),
			`${this.randomId}.${extension}`
		].join('/');
	}

	formatDate(date) {
		return [
			date.getFullYear(),
			String(date.getMonth() + 1).padStart(2, '0'),
			String(date.getDate()).padStart(2, '0')
		].join('/');
	}

	parseMessageAttributes(attributes, sqsName) {

		const parsedAttributes = {};

		if(this.session?.clientCode) {
			parsedAttributes['janis-client'] = {
				DataType: 'String',
				StringValue: this.session.clientCode
			};
		}

		if(attributes) {
			Object.entries(attributes).forEach(([key, value]) => {
				parsedAttributes[key] = {
					DataType: Array.isArray(value) ? 'String.Array' : 'String',
					StringValue: Array.isArray(value) ? JSON.stringify(value) : value
				};
			});
		}

		parsedAttributes.sqsName = {
			DataType: 'String',
			StringValue: sqsName
		};

		return parsedAttributes;
	}

	getSqsNameFromArn(arn) {

		const arnParts = arn.split(':');
		let sqsName = arnParts[arnParts.length - 1];

		if(sqsName === 'fifo')
			[sqsName] = sqsName.split('.');

		return sqsName;
	}

};
