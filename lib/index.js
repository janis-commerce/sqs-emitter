'use strict';

const SqsEmitter = require('./sqs-emitter');
const SqsEmitterError = require('./sqs-emitter-error');
const sqsPermissions = require('./helpers/permissions');

module.exports = {
	SqsEmitter,
	SqsEmitterError,
	sqsPermissions
};
