'use strict';

const assert = require('assert');
const idHelper = require('../../lib/helpers/id-helper');

describe('randomValue', () => {

	it('Should return a random value of the specified length', () => {
		const length = 8;
		const result = idHelper.randomValue(length);

		// Check that the result has the correct length
		assert.strictEqual(result.length, length);

		// Check that the result contains only characters from the expected set
		const isValid = /^[A-Z1-9]+$/.test(result);
		assert.strictEqual(isValid, true, `Expected characters from 'A-Z1-9', but got ${result}`);
	});
});
