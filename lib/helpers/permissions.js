/* eslint-disable no-template-curly-in-string */

'use strict';

const sqsPermissions = [
	{
		action: ['ssm:GetParameter'],
		resource: 'arn:aws:ssm:${aws:region}:*:parameter/shared/internal-storage'
	},
	{
		action: ['ram:ListResources'],
		resource: '*'
	}
];

module.exports = sqsPermissions.map(statement => ['iamStatement', statement]);
