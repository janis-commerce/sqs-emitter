'use strict';

const logger = require('lllog')();

const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const SqsEmitterError = require('../sqs-emitter-error');

/**
 * @typedef {Object} BucketInfo
 * @property {string} bucketName - The name of the S3 bucket
 * @property {string} region - The region of the S3 bucket
**/

module.exports = class S3Uploader {

	/**
	 * Uploads the content to the S3 bucket at the specified path. In case the upload fails, no error is thrown.
	 *
	 * @param {Object} bucket - The S3 bucket configuration.
	 * @param {string} bucket.bucketName - The name of the S3 bucket.
	 * @param {string} bucket.region - The region of the S3 bucket.
	 * @param {string} contentS3Path - The path in the S3 bucket where the content will be uploaded.
	 * @param {string} body - The payload to upload s3 bucket
	 * @returns {Promise<Object|undefined>} - The result from S3 after the upload or `undefined` if an error occurs.
	 */
	static async uploadToBucket(bucket, contentS3Path, body) {

		try {

			const s3Client = new S3Client({ region: bucket.region });

			return await s3Client.send(new PutObjectCommand({
				Bucket: bucket.bucketName,
				Key: contentS3Path,
				Body: body
			}));

		} catch(error) {
			logger.error(`Error uploading to bucket ${bucket.bucketName} in region ${bucket.region}: ${error.message}`);
		}
	}

	/**
	 * Tries to upload content to a list of S3 buckets in order. If the upload to the first bucket fails,
	 * it will attempt to upload to the next bucket in the list until a successful upload occurs or all
	 * buckets have been tried. If all attempts fail, an error is thrown.
	 *
	 * @param {BucketInfo[]} buckets - An array of S3 bucket configurations, where each object contains the bucket's details: bucketName, region.
	 * @param {string} contentS3Path - The path in the S3 bucket where the content will be uploaded.
	 * @param {string} body - The content to be uploaded to the S3 bucket.
	 * @returns {Promise<BucketInfo>} - Resolves with the bucket information of the successful upload.
	 * @throws {SqsEmitterError} - Throws an error if the upload fails for all provided buckets.
	 */
	static async uploadContentS3Path(buckets, contentS3Path, body) {

		for(const bucket of buckets) {

			const success = await this.uploadToBucket(bucket, contentS3Path, body);

			if(success)
				return bucket;
		}

		throw new SqsEmitterError('Failed to upload to all provided buckets', SqsEmitterError.codes.S3_ERROR);
	}

};
