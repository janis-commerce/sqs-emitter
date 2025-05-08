'use strict';

const logger = require('lllog')();
const { SQSClient, SendMessageBatchCommand, SendMessageCommand } = require('@aws-sdk/client-sqs');

const AsyncWithConcurrency = require('./helpers/async-with-concurrency');
const SqsEmitterError = require('./sqs-emitter-error');
const ParameterStore = require('./helpers/parameter-store');
const S3Uploader = require('./helpers/s3-uploader');
const { pickProperties } = require('./helpers/pick-properties');
const { randomValue } = require('./helpers/id-helper');

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

	async publishEvent(sqsUrl, event) {

		const sqsName = this.getSqsNameFromUrl(sqsUrl);
		const parsedEvent = this.formatSQSEvent(event, sqsName);
		const parsedEventSize = JSON.stringify(parsedEvent).length;

		const { extraProperties, ...parsedEventBase } = parsedEvent;

		let formattedSqsMessage = parsedEventBase;

		if(parsedEventSize > SQS_MESSAGE_LIMIT_SIZE) {

			const formattedEvent = await this.formatAndUploadEventWithS3Content(parsedEvent);

			formattedSqsMessage = formattedEvent;
		}

		const sqsResponse = await this.sqs.send(new SendMessageCommand({
			...formattedSqsMessage,
			QueueUrl: sqsUrl
		}));

		return {
			messageId: sqsResponse.MessageId,
			...sqsResponse.SequenceNumber && { sequenceNumber: sqsResponse.SequenceNumber }
		};
	}

	async publishEvents(sqsUrl, events) {

		const parsedEvents = this.parseEvents(events, sqsUrl);

		const asyncWithConcurrency = new AsyncWithConcurrency(async batch => {

			const formattedSqsPromises = [];
			const formattedSqsBatch = [];

			for(const { limitExceeded, extraProperties, ...parsedEvent } of batch) {

				if(!limitExceeded) {
					formattedSqsBatch.push(parsedEvent);
					continue;
				}

				const promise = this.formatAndUploadEventWithS3Content({ ...parsedEvent, extraProperties });

				formattedSqsPromises.push(promise);
			}

			const formattedSqsBatchEntries = await Promise.all(formattedSqsPromises);

			const s3Failed = [];

			for(const formattedSqsBatchEntry of formattedSqsBatchEntries) {

				if(formattedSqsBatchEntry.Error)
					s3Failed.push(formattedSqsBatchEntry.Error);
				else
					formattedSqsBatch.push(formattedSqsBatchEntry);

			}

			let sqsResponse = {};

			if(formattedSqsBatch.length) {
				sqsResponse = await this.sqs.send(new SendMessageBatchCommand({
					Entries: formattedSqsBatch,
					QueueUrl: sqsUrl
				}));
			}

			sqsResponse.Failed ??= [];

			for(const s3FailedEntry of s3Failed)
				sqsResponse.Failed.push(s3FailedEntry);

			return sqsResponse;

		}, MAX_CONCURRENCY);

		/** @type {import('@aws-sdk/client-sqs').SendMessageBatchCommand[]} */
		const results = await asyncWithConcurrency.run(parsedEvents);

		return this.formatSQSResponse(results);
	}

	formatSQSResponse(results) {

		const response = {
			successCount: 0,
			failedCount: 0,
			success: [],
			failed: []
		};

		results.forEach(result => {

			if(result.Successful) {

				response.successCount += result.Successful.length;

				result.Successful.forEach(success => response.success.push({
					Id: success.Id,
					messageId: success.MessageId,
					...success.SequenceNumber && { sequenceNumber: success.SequenceNumber }
				}));
			}

			if(result.Failed?.length) {

				response.failedCount += result.Failed.length;

				result.Failed.sort((a, b) => Number(a.Id) - Number(b.Id));

				response.failed.push(...result.Failed);
			}

		});

		return response;
	}

	parseEvents(events, sqsUrl) {

		const parsedEvents = [
			[]
		];

		let currentBatchIndex = 0;
		let currentBatchSize = 0;

		let eventIndex = 0;

		const sqsName = this.getSqsNameFromUrl(sqsUrl);

		for(const event of events) {

			eventIndex++;

			let parsedEvent = this.formatSQSEvent(event, sqsName, eventIndex);

			let parsedEventSize = JSON.stringify(parsedEvent).length;

			[parsedEvent, parsedEventSize] = this.handleEventSizeLimit(parsedEvent, parsedEventSize);

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

	async formatAndUploadEventWithS3Content(parsedEvent) {

		const { limitExceeded, extraProperties: { payloadFixedProperties, contentS3Path }, ...parsedEventBase } = parsedEvent;

		const bucketList = await ParameterStore.getParameterValue();

		const bucketInfo = await S3Uploader.uploadContentS3Path(bucketList, contentS3Path, parsedEvent.MessageBody);

		if(!bucketInfo) {
			return {
				Error: {
					Id: parsedEvent.Id,
					Message: 'Failed to upload to all provided s3 buckets',
					Code: SqsEmitterError.codes.S3_ERROR
				}
			};
		}

		const sqsFixedContent = this.formatBodyWithContentS3Path(parsedEventBase, payloadFixedProperties, contentS3Path, bucketInfo);

		return { ...parsedEventBase, MessageBody: JSON.stringify(sqsFixedContent) };
	}

	/**
		* Formats the SQS message body with the content location in S3.
		* This method generates an object that includes the content location in S3 and, optionally,
		* fixed payload properties extracted from the original message.
		*
		* @param {Object} parsedEvent - The processed SQS event.
		* @param {string[]} payloadFixedProperties - List of specific properties to extract from the original message.
		* @param {string} contentS3Path - The path to the content in S3.
		* @param {Object} bucketInfo - Information about the S3 bucket.
		* @param {string} bucketInfo.region - The region of the S3 bucket.
		* @param {string} bucketInfo.bucketName - The name of the S3 bucket.
		* @returns {Object} An object containing the content location in S3 and the fixed payload properties (if specified).
		*/
	formatBodyWithContentS3Path(parsedEvent, payloadFixedProperties, contentS3Path, bucketInfo) {
		return {
			contentS3Location: {
				path: contentS3Path,
				...bucketInfo && { bucketName: bucketInfo.bucketName, region: bucketInfo.region }
			},
			...payloadFixedProperties?.length && {
				...pickProperties(JSON.parse(parsedEvent.MessageBody), payloadFixedProperties)
			}
		};
	}

	formatSQSEvent(event, sqsName, eventIndex) {

		const parsedAttributes = this.parseMessageAttributes(event.attributes, sqsName);
		const extraProperties = this.parseExtraProperties(event, sqsName);

		return {
			...eventIndex && { Id: `${eventIndex}` },
			MessageBody: JSON.stringify(event.content),
			MessageAttributes: parsedAttributes,
			...event.subject && { Subject: event.subject },
			...event.messageDeduplicationId && { MessageDeduplicationId: event.messageDeduplicationId },
			...event.messageGroupId && { MessageGroupId: event.messageGroupId },
			...event.messageStructure && { MessageStructure: event.messageStructure },
			extraProperties
		};
	}

	/**
 	 * Handles the event size limit by formatting the event with an S3 content path.
 	 * If the event size exceeds the limit, it will be formatted with an S3 content path.
	 *
	 * estimatedBucketInfoSize: The estimated size of the bucket information (bucketName and region).
	 * This is calculated only for batch events and overestimated to avoid future issues with larger bucket names or regions.
	 *
	 * Example:
	 * "bucketName":"janis-internal-storage-beta-us-east-1" // Approx. 60 characters max
	 * "region":"us-east-1" // Approx. 25 characters max
	 *
	 * Extra characters: Approx. 5 characters
	 * Total estimated size: ~90 characters
	 *
 	 * @param {Object} parsedEvent - The event object that has been parsed previously.
 	 * @param {number} parsedEventSize - The size of the parsed event in bytes.
 	 * @returns {[Object, number]} - Returns an array where the first element is the modified parsed event,
 	 * and the second element is the updated event size.
 	 */
	handleEventSizeLimit(parsedEvent, parsedEventSize) {

		const {
			extraProperties: { payloadFixedProperties, contentS3Path }
		} = parsedEvent;

		if(parsedEventSize > SQS_MESSAGE_LIMIT_SIZE) {

			const contentFixed = this.formatBodyWithContentS3Path(parsedEvent, payloadFixedProperties, contentS3Path);

			const estimatedBucketInfoSize = 90;

			// Recalculate event size after formatting
			parsedEventSize = JSON.stringify(contentFixed).length + estimatedBucketInfoSize;

			parsedEvent.limitExceeded = true;

			logger.info('Parsed event size exceeds the 256KB limit. It will be sent with an S3 content path: ', contentS3Path);
		}

		return [parsedEvent, parsedEventSize];
	}

	getContentS3Path(sqsName) {

		const now = new Date();
		const extension = 'json';

		return [
			'sqsContent',
			`${this.session?.clientCode || 'storage'}`,
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

		const parsedAttributes = {
			sqsName: {
				DataType: 'String',
				StringValue: sqsName
			}
		};

		if(this.session?.clientCode) {
			parsedAttributes['janis-client'] = {
				DataType: 'String',
				StringValue: this.session.clientCode
			};
		}

		if(attributes) {

			Object.entries(attributes).forEach(([key, value]) => {

				let StringValue;

				if(Array.isArray(value))
					StringValue = JSON.stringify(value);
				else
					StringValue = String(value);

				parsedAttributes[key] = {
					DataType: Array.isArray(value) ? 'String.Array' : 'String',
					StringValue
				};
			});
		}

		return parsedAttributes;
	}

	parseExtraProperties(event, sqsName) {
		return {
			...event.payloadFixedProperties && { payloadFixedProperties: event.payloadFixedProperties },
			contentS3Path: this.getContentS3Path(sqsName)
		};
	}

	getSqsNameFromUrl(queueUrl) {

		const isValidSqsUrl = this.isValidSqsUrl(queueUrl);

		if(!isValidSqsUrl)
			throw new SqsEmitterError(`Invalid SQS URL: ${queueUrl}`, SqsEmitterError.codes.INVALID_QUEUE_URL);

		const queueUrlParts = queueUrl.split('/');
		const sqsName = queueUrlParts[queueUrlParts.length - 1];

		if(sqsName.endsWith('.fifo'))
			return sqsName.substring(0, sqsName.length - 5);

		return sqsName;
	}

	isValidSqsUrl(url) {
		const pattern = /^https:\/\/sqs\.[a-zA-Z0-9-]+\.amazonaws\.com\/\d{12}\/[a-zA-Z0-9_-]+(\.fifo)?$/;
		return pattern.test(url);
	}
};
