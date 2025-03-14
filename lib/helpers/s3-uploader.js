/* istanbul ignore file */

'use strict';

const logger = require('lllog')();
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const AssumeRole = require('./assume-role');

module.exports = class S3Uploader {

	/**
 	 * Uploads content to a specified S3 bucket.
 	 * This method assumes the necessary AWS credentials using a role ARN provided in the bucket configuration,
 	 * then uploads the content to the S3 bucket at the specified path. In case the upload fails, no error is thrown
 	 * because a retry mechanism is in place that will attempt to upload the content to a provisional bucket in another region.
 	 *
 	 * @param {Object} bucket - The S3 bucket configuration.
 	 * @param {string} bucket.bucketName - The name of the S3 bucket.
 	 * @param {string} bucket.region - The region of the S3 bucket.
 	 * @param {string} bucket.roleArn - The ARN of the IAM role to assume for credentials.
 	 * @param {string} s3ContentPath - The path in the S3 bucket where the content will be uploaded.
 	 * @param {string} body - The payload to upload s3 bucket
 	 * @returns {Promise<Object|undefined>} - The result from S3 after the upload or `undefined` if an error occurs.
 	 */
	static async uploadToBucket(bucket, s3ContentPath, body) {

		try {

			const credentials = await AssumeRole.getCredentials(bucket.roleArn);

			if(!credentials)
				return;

			const s3Client = new S3Client({ region: bucket.region, credentials });

			const data = await s3Client.send(new PutObjectCommand({
				Bucket: bucket.bucketName,
				Key: s3ContentPath,
				Body: body
			}));

			return data;
		} catch(error) {
			logger.error(`Error uploading to bucket ${bucket.bucketName} in region ${bucket.region}: ${error.message}`);
		}
	}

	/**
 * Handle uploads content to a specified S3 content path, attempting first on the default bucket, and then retrying on a provisional bucket if the first attempt fails.
 * This method takes an array of buckets (default and provisional) in different regions and tries to upload the content to the default bucket first.
 * If the upload fails, it retries with the provisional bucket. If both attempts fail, it rejects the promise with an error.
 *
 * @async
 * @param {Object[]} buckets - An array containing two S3 bucket configurations: [defaultBucket, provisionalBucket].
 * @param {Object} buckets[].bucketName - The name of the S3 bucket.
 * @param {Object} buckets[].region - The region of the S3 bucket.
 * @param {Object} buckets[].roleArn - The ARN of the IAM role to assume for credentials.
 * @param {string} s3ContentPath - The path in the S3 bucket where the content will be uploaded.
 * @param {string} body - The payload to upload to s3 bucket
 * @returns {Promise<void>} - Resolves if the content is successfully uploaded to either the default or provisional bucket.
 * Rejects with an error if both attempts fail.
 *
 * @throws {Error} - If the upload fails for both the default and provisional buckets, an error is thrown.
 */
	static async uploadS3ContentPath(buckets, s3ContentPath, body) {

		const [defaultBucket, provisionalBucket] = buckets;

		const defaultSuccess = await this.uploadToBucket(defaultBucket, s3ContentPath, body);

		if(defaultSuccess)
			return;

		const provisionalSuccess = await this.uploadToBucket(provisionalBucket, s3ContentPath, body);

		if(provisionalSuccess)
			return;

		throw new Error('Failed to upload to both default and provisional buckets');
	}

};
