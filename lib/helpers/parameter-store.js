/* eslint-disable import/no-extraneous-dependencies */

'use strict';

const { SSMClient, GetParameterCommand } = require('@aws-sdk/client-ssm');
const { RAMClient, ListResourcesCommand } = require('@aws-sdk/client-ram');
const memoize = require('micro-memoize');
const SqsEmitterError = require('../sqs-emitter-error');

/**
 * @typedef {Object} S3BucketConfig
 * @property {string} bucketName - The name of the S3 bucket.
 * @property {string} roleArn - The ARN of the IAM role associated with the bucket.
 * @property {string} region - The AWS region where the bucket is located.
 * @property {boolean} [default] - Indicates if this bucket is the default (optional).
 */

class ParameterStore {

	/**
		* @private
		* @static
		*/
	static get parameterName() {
		return '/shared/internal-storage';
	}

	static clearCache() {
		ParameterStore.getParameterValue.cache.keys.length = [];
		ParameterStore.getParameterValue.cache.values.length = [];
		ParameterStore.getParameterArnFromRAM.cache.keys.length = [];
		ParameterStore.getParameterArnFromRAM.cache.values.length = [];
	}

	/**
		* Retrieves a parameter value from AWS SSM Parameter Store using its ARN.
		* This method first obtains the ARN of the parameter by invoking `getParameterArnFromRAM`.
		* Then, it attempts to fetch the parameter value from the SSM Parameter Store.
		* The value is decrypted if it's stored as a secure string.
		*
		* @returns {Promise<Array<S3BucketConfig> | undefined>}
		* Returns a parsed JSON array of objects, where each object contains
		* information about S3 buckets, such as the `bucketName`, `roleArn`, `region`, and an optional `default` flag.
		* If the parameter retrieval fails, it logs an error and returns `undefined`.
		* @throws {Error} - If the `getParameterArnFromRAM` method fails, or the SSM command fails.
		*/
	static async getParameterValue() {

		const parameterArn = await this.getParameterArnFromRAM();

		try {

			const ssmClient = new SSMClient();

			const params = {
				Name: parameterArn,
				WithDecryption: true
			};

			const response = await ssmClient.send(new GetParameterCommand(params));

			return JSON.parse(response.Parameter.Value);
		} catch(error) {
			throw new SqsEmitterError(`Unable to get parameter with arn ${parameterArn} - ${error.message}`, SqsEmitterError.codes.SSM_ERROR);
		}
	}

	/**
		* Retrieves the ARN of a specific parameter from AWS Resource Access Manager (RAM) by filtering
		* the shared resources from 'OTHER-ACCOUNTS' that include the specified parameter name.
		*
		* @returns {Promise<string>} The ARN of the filtered resource that matches the parameter name.
		* @throws {Error} If there is an error while listing RAM resources or no resources match the parameter.
		*/
	static async getParameterArnFromRAM() {

		try {

			const command = new ListResourcesCommand({ resourceOwner: 'OTHER-ACCOUNTS' });

			const ramClient = new RAMClient({ region: 'us-east-1' });

			const response = await ramClient.send(command);

			const filteredResources = response.resources.filter(
				resource => resource.arn.includes(this.parameterName)
			);

			if(!filteredResources.length)
				throw new Error(`Unable to find resources with parameter ${this.parameterName} in the ARN`);

			return filteredResources[0].arn;

		} catch(error) {
			throw new SqsEmitterError(`Resource Access Manager Error: ${error.message}`, SqsEmitterError.codes.RAM_ERROR);
		}
	}

}

ParameterStore.getParameterValue = memoize(ParameterStore.getParameterValue, { isPromise: true });
ParameterStore.getParameterArnFromRAM = memoize(ParameterStore.getParameterArnFromRAM, { isPromise: true });
module.exports = ParameterStore;
