'use strict';

const memoize = require('micro-memoize');
const { customAlphabet } = require('nanoid');

const randomValue = memoize(digits => customAlphabet('ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789', digits));

module.exports = {
	randomValue: digits => randomValue(digits)()
};
