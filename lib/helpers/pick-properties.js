'use strict';

module.exports.pickProperties = (object, keys) => {

	const result = {};

	for(const key of keys) {
		if(typeof object[key] !== 'undefined')
			result[key] = object[key];
	}

	return result;
};
