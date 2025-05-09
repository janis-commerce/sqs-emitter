/* eslint-disable no-loop-func */

'use strict';

module.exports = class AsyncWithConcurrency {

	constructor(workerFn, maxConcurrency) {
		this.workerFn = workerFn;
		this.maxConcurrency = maxConcurrency;
		this.activeTasks = 0;
	}

	async run(queue) {

		// Make this copy to avoid modifying the original queue
		const queueCopy = [...queue];

		const results = [];
		const promises = new Map();
		let itemIndex = 0;

		while(queueCopy.length || this.activeTasks) {

			const chunkConcurrency = Math.min(this.maxConcurrency - this.activeTasks, queueCopy.length);

			const tasks = queueCopy.splice(0, chunkConcurrency);

			tasks.forEach(task => {
				promises.set(itemIndex, this.processTask(task, itemIndex));
				itemIndex++;
				this.activeTasks++;
			});

			const result = await Promise.race(promises.values());

			this.activeTasks--;

			results[result.index] = result.value;

			promises.delete(result.index);
		}

		return results;
	}

	async processTask(task, itemIndex) {

		const taskResult = await this.workerFn(task);

		return {
			index: itemIndex,
			value: taskResult
		};

	}
};
