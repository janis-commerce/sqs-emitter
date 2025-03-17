'use strict';

const assert = require('assert');
const { pickProperties } = require('../../lib/helpers/pick-properties');

describe('pickProperties', () => {

	it('Should pick specified properties when they exist', () => {
		const input = { a: 1, b: 2, c: 3 };
		const keys = ['a', 'b'];
		const expected = { a: 1, b: 2 };
		const result = pickProperties(input, keys);
		assert.deepStrictEqual(result, expected);
	});

	it('should only pick existing properties', () => {
		const input = { a: 1, c: 3 };
		const keys = ['a', 'b', 'c'];
		const expected = { a: 1, c: 3 };
		const result = pickProperties(input, keys);
		assert.deepStrictEqual(result, expected);
	});

	it('should return empty object when input is empty', () => {
		const input = {};
		const keys = ['a', 'b'];
		const expected = {};
		const result = pickProperties(input, keys);
		assert.deepStrictEqual(result, expected);
	});

	it('should return empty object when keys array is empty', () => {
		const input = { a: 1, b: 2 };
		const keys = [];
		const expected = {};
		const result = pickProperties(input, keys);
		assert.deepStrictEqual(result, expected);
	});

	it('should skip falsy values', () => {
		const input = {
			a: 0, b: null, c: undefined, d: false, e: 1
		};
		const keys = ['a', 'b', 'c', 'd', 'e'];
		const expected = { e: 1 };
		const result = pickProperties(input, keys);
		assert.deepStrictEqual(result, expected);
	});

});
