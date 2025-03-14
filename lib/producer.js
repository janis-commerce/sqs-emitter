/* istanbul ignore file */

'use strict';

const SQSWrapper = require('../../../lib/aws/sqs');

const sqsClient = new SQSWrapper();

const MAX_MESSAGES_PER_BATCH = 10;

/**
 * @typedef {object} ProducedMessageSuccess
 * @property {object} message The original message
 * @property {string} messageId The message ID in the queue
 */

/**
 * @typedef {object} ProducedMessageFailed
 * @property {string} message The error message
 * @property {string} [code] The error code
 * @property {boolean} [isClientError] Indicates if the error is client or server responsibility
 */

/**
 * @typedef {object} ProducedMessageFailed
 * @property {object} message The original message
 * @property {ProducedMessageFailed} error The error detail
 */

/**
 * @typedef {ProducedMessageSuccess|ProducedMessageFailed} ProducedMessagesResult
 */

/**
 * @typedef {object} ProducedMessagesOutput
 * @property {number} successfulCount
 * @property {number} failedCount
 * @property {ProducedMessagesResult[]} results
 */

/**
 * @typedef {object} FormattedSQSMessage
 * @property {string} Id
 * @property {Object} MessageAttributes janis-client information
 * @property {string} MessageBody
 */

module.exports = class QueueProducer {

	/**
	 *
	 * @param {string} queueUrl
	 * @param {object[]} messageBodies
	 * @returns {Promise<ProducedMessagesOutput>}
	 */
	async sendMessages(queueUrl, messageBodies) {

		const messages = this.mapToSQSFormat(messageBodies);

		const promises = [];

		for(let i = 0; i < messages.length; i += MAX_MESSAGES_PER_BATCH) {

			const chunk = messages.slice(i, i + MAX_MESSAGES_PER_BATCH);

			promises.push(sqsClient.sendMessages(queueUrl, chunk));
		}

		const results = await Promise.allSettled(promises);

		return this.formatSQSResponse(results, messages);
	}

	/**
	 * @param {Array<Object>} messages message bodies
	 * @returns {Array<FormattedSQSMessage>}
	*/
	mapToSQSFormat(messages) {

		return messages.map((message, index) => ({
			Id: `message-${index}`,
			MessageAttributes: {
				'janis-client': {
					DataType: 'String',
					StringValue: this.session.clientCode
				}
			},
			MessageBody: JSON.stringify(message)
		}));
	}

	formatSQSResponse(results, messages) {

		const initialState = {
			successfulCount: 0,
			failedCount: 0,
			results: []
		};

		return results.reduce((response, result, index) => {

			const batchMessages = messages.slice(MAX_MESSAGES_PER_BATCH * index, MAX_MESSAGES_PER_BATCH * (index + 1));

			// Full request fail. All the chunk must be retried.
			if(result.status === 'rejected') {

				response.failedCount += batchMessages.length;

				response.results.push(...batchMessages.map(message => ({
					message: JSON.parse(message.MessageBody),
					error: {
						message: result.reason?.message
					}
				})));

				return response;
			}

			if(result.value.Successful?.length) {

				response.successfulCount += result.value.Successful.length;

				response.results.push(...result.value.Successful.map(({ Id, MessageId }) => {

					const idResult = Id.replace('message-', '');

					return {
						message: JSON.parse(batchMessages[idResult].MessageBody),
						messageId: MessageId
					};
				}));
			}

			if(result.value.Failed?.length) {

				response.failedCount += result.value.Failed.length;

				response.results.push(...result.value.Failed.map(({ Id, Message, Code, SenderFault }) => {

					const message = Id.replace('message-', '');

					return {
						message: JSON.parse(batchMessages[message].MessageBody),
						error: {
							message: Message || Code,
							code: Code,
							isClientError: !!SenderFault
						}
					};
				}));
			}

			return response;

		}, initialState);
	}
};
