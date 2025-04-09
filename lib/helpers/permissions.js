'use strict';

const sqsPermissions = [
	{
		action: ['ssm:GetParameter'],
		resource: 'arn:aws:ssm:us-east-1:*:parameter/shared/internal-storage'
	},
	{
		action: ['ram:ListResources'],
		resource: '*'
	}
];

module.exports = sqsPermissions.map(statement => ['iamStatement', statement]);
