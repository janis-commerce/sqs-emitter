'use strict';

const logger = require('lllog')();
const { SQSClient, SendMessageBatchCommand, SendMessageCommand } = require('@aws-sdk/client-sqs');

const AsyncWithConcurrency = require('./helpers/async-with-concurrency');
const ParameterStore = require('./helpers/parameter-store');
const S3Uploader = require('./helpers/s3-uploader');
const { randomValue } = require('./helpers/id-helper');
const { pickProperties } = require('./helpers/pick-properties');
const SqsEmitterError = require('./sqs-emitter-error');
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

	async publishEvent(sqsUrl, event) {

		const sqsName = this.getSqsNameFromUrl(sqsUrl);
		let parsedEvent = this.formatSQSEvent(event, sqsName);
		let parsedEventSize = JSON.stringify(parsedEvent).length;

		const {
			extraProperties: { s3ContentPath, payloadFixedProperties }, ...parsedEventBase
		} = parsedEvent;

		let formattedSqsMessage = parsedEventBase;

		[parsedEvent, parsedEventSize] = this.handleEventSizeLimit(parsedEvent, parsedEventSize);

		if(parsedEvent.limitExceeded) {

			const bucketList = await ParameterStore.getParameterValue();

			const sqsFixedContent = this.formatBodyWithS3ContentPath(parsedEvent, s3ContentPath, payloadFixedProperties);

			formattedSqsMessage = {
				...parsedEventBase,
				MessageBody: JSON.stringify(sqsFixedContent)
			};

			await S3Uploader.uploadS3ContentPath(bucketList, s3ContentPath, parsedEvent.MessageBody);
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

			const promises = [];
			const formattedSqsBatch = [];

			for(const { limitExceeded, extraProperties: { payloadFixedProperties, s3ContentPath }, ...parsedEvent } of batch) {

				if(!limitExceeded) {
					formattedSqsBatch.push(parsedEvent);
					continue;
				}

				const bucketList = await ParameterStore.getParameterValue();

				const sqsFixedContent = this.formatBodyWithS3ContentPath(parsedEvent, s3ContentPath, payloadFixedProperties);

				formattedSqsBatch.push({ ...parsedEvent, MessageBody: JSON.stringify(sqsFixedContent) });

				promises.push(S3Uploader.uploadS3ContentPath(bucketList, s3ContentPath, parsedEvent.MessageBody));
			}

			await Promise.all(promises);

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

	formatBodyWithS3ContentPath(parsedEvent, s3ContentPath, payloadFixedProperties) {
		return {
			s3ContentPath,
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
			...parsedAttributes && { MessageAttributes: parsedAttributes },
			...extraProperties && { extraProperties }
		};
	}

	handleEventSizeLimit(parsedEvent, parsedEventSize) {

		const {
			extraProperties: { payloadFixedProperties, s3ContentPath }
		} = parsedEvent;

		if(parsedEventSize > SQS_MESSAGE_LIMIT_SIZE) {

			const contentFixed = this.formatBodyWithS3ContentPath(parsedEvent, s3ContentPath, payloadFixedProperties);

			parsedEventSize = JSON.stringify(contentFixed).length;

			parsedEvent.limitExceeded = true;

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

	parseExtraProperties(event, sqsName) {

		if(!this.session?.clientCode)
			throw new SqsEmitterError('The session must have a clientCode', SqsEmitterError.codes.MISSING_CLIENT_CODE);

		return {
			...event.payloadFixedProperties && { payloadFixedProperties: event.payloadFixedProperties },
			s3ContentPath: this.getS3ContentPath(sqsName)
		};
	}

	getSqsNameFromUrl(queueUrl) {

		const isValidSqsUrl = this.isValidSqsUrl(queueUrl);

		if(!isValidSqsUrl)
			throw new SqsEmitterError(`Invalid SQS URL: ${queueUrl}`, SqsEmitterError.codes.INVALID_QUEUE_URL);

		const queueUrlParts = queueUrl.split('/');
		let sqsName = queueUrlParts[queueUrlParts.length - 1];

		if(sqsName.search('fifo') !== -1)
			[sqsName] = sqsName.split('.');

		return sqsName;
	}

	isValidSqsUrl(url) {
		const pattern = /^https:\/\/sqs\.[a-zA-Z0-9-]+\.amazonaws\.com\/\d{12}\/[a-zA-Z0-9]+(\.fifo)?$/;
		return pattern.test(url);
	}
};
