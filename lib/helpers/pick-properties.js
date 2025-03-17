'use strict';

module.exports.pickProperties = (object, keys) => {

	const result = {};

	for(const key of keys) {
		if(object[key])
			result[key] = object[key];
	}

	return result;
};
