'use strict';

class SqsEmitterError extends Error {

	static get codes() {
		return {
			MISSING_CLIENT_CODE: 'MISSING_CLIENT_CODE',
			INVALID_QUEUE_URL: 'INVALID_QUEUE_URL',
			RAM_ERROR: 'RAM_ERROR',
			SSM_ERROR: 'SSM_ERROR',
			STS_ERROR: 'STS_ERROR',
			SQS_ERROR: 'SQS_ERROR',
			S3_ERROR: 'S3_ERROR'
		};
	}

	constructor(err, code) {
		super(err);
		this.message = err.message || err;
		this.code = code;
		this.name = 'SqsEmitterError';
	}
}

module.exports = SqsEmitterError;
