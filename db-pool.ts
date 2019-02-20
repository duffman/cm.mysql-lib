/**
 * Copyright (c) Patrik Forsberg <patrik.forsberg@coldmind.com> - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * February 2019
 */

export interface IDBPool {
}

export class DBPool implements IDBPool {
	constructor() {}

	public getPoolConnection() {
	/*	function handle_database (req, type, callback) {
			async.waterfall([
				function (callback) {
					pool.getConnection(function (err, connection) {
						if (err) {
							// if there is error, stop right away.
							// This will stop the async code execution and goes to last function.
							callback(true);
						} else {
							callback(null, connection);
						}
					});
				},
				function (connection, callback) {

					callback(null, connection, SQLquery);
				},
				function (connection, SQLquery, callback) {
					connection.query(SQLquery, function (err, rows) {
						connection.release();
						if (!err) {
							if (type === "login") {
								callback(rows.length === 0 ? false : rows[0]);
							} else if (type === "getStatus") {
								callback(rows.length === 0 ? false : rows);
							} else if (type === "checkEmail") {
								callback(rows.length === 0 ? false : true);
							} else {
								callback(false);
							}
						} else {
							// if there is error, stop right away.
							// This will stop the async code execution and goes to last function.
							callback(true);
						}
					});
				}
			], function (result) {
				// This function gets call after every async task finished.
				if (typeof(result) === "boolean" && result === true) {
					callback(null);
				} else {
					callback(result);
				}
			});
		}*/
	}
}
