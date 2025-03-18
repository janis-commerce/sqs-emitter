'use strict';

const { STSClient, AssumeRoleCommand } = require('@aws-sdk/client-sts');
const SqsEmitterError = require('../sqs-emitter-error');

module.exports = class AssumeRole {

	/**
    * @private
    * @static
    */
	static get roleSessionName() {
		return process.env.JANIS_SERVICE_NAME;
	}

	/**
    * @private
    * @static
    */
	static get roleSessionDuration() {
		return 1800; // 30 minutes
	}

	/**
 	 * Retrieves temporary AWS Role credentials by assuming an IAM role.
 	 * This method uses the AWS Security Token Service (STS) to assume a specified IAM role and return temporary credentials.
	 * These credentials can be used to authenticate requests to other AWS services on behalf of the assumed role.
 	 *
 	 * @private
 	 * @static
 	 * @async
 	 * @param {string} roleArn - The Amazon Resource Name (ARN) of the AWS role to assume.
 	 * @returns {Promise<object>} An object containing the temporary credentials for the assumed role:
 	 *   - `accessKeyId` {string}: The access key ID associated with the role.
 	 *   - `secretAccessKey` {string}: The secret access key associated with the role.
 	 *   - `sessionToken` {string}: The temporary session token.
 	 *   - `expiration` {Date}: The expiration date of the temporary credentials.
 	 */
	static async getCredentials(roleArn) {

		try {

			const stsClient = new STSClient();

			const assumedRole = await stsClient.send(new AssumeRoleCommand({
				RoleArn: roleArn,
				RoleSessionName: this.roleSessionName,
				DurationSeconds: this.roleSessionDuration
			}));

			const { Credentials } = assumedRole;

			return {
				accessKeyId: Credentials.AccessKeyId,
				secretAccessKey: Credentials.SecretAccessKey,
				sessionToken: Credentials.SessionToken,
				expiration: Credentials.Expiration
			};

		} catch(err) {
			throw new SqsEmitterError(`Error while trying to assume role arn ${roleArn}: ${err.message}`, SqsEmitterError.codes.ASSUME_ROLE_ERROR);
		}
	}
};
