'use strict';

const sqsPermissions = [
	{
		action: ['ssm:GetParameter'],
		resource: 'arn:aws:ssm:us-east-1:*:parameter/shared/internal-storage'
	},
	{
		action: ['ram:ListResources'],
		resource: '*'
	},
	{
		action: 'sts:AssumeRole',
		resource: 'arn:aws:iam::*:role/LambdaRemoteStorage'
	}
];

module.exports = sqsPermissions.map(statement => ['iamStatement', statement]);
