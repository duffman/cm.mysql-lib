/**
 * Copyright (C) Patrik Forsberg <patrik.forsberg@coldmind.com> - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

import * as mysql                 from "mysql";
import { SQLTableData }           from "./sql-table-data";
import { IDbResult }              from "./db-result";
import { DbResult }               from "./db-result";
import { DbLogger }               from "./db-logger";
import { Connection }             from 'mysql';
import { DbResultParser } from "@db/db-result-parser";

const log = console.log;

export interface IConnectionSettings {
	host: string;
	user: string;
	password: string;
	database: string;
}

export interface IOkPacket {
	fieldCount: number;
	affectedRows: number;
	insertId: number;
	serverStatus: number;
	warningCount: number;
	message: string;
	protocol41: boolean;
	changedRows: number;
}

enum DbState {
	Unset,
	Connected,
	Disconnected
}

export interface IDbKernel {
	createConnection(openConnection: boolean = true): Connection;
	dbQuery(sql: string): Promise<IDbResult>;
}

export class DbKernel implements IDbKernel {
	private connSettings: IConnectionSettings;
	private connLost: boolean = false;

	constructor() {}

	public assignSettings(settings: IConnectionSettings): void {
		this.connSettings = settings;
	}

	public createConnection(openConnection: boolean = true): Connection {
		let conn: Connection;

		try {
			conn = mysql.createConnection({
				host: this.connSettings.host,
				user: this.connSettings.user,
				password: this.connSettings.password,
				database: this.connSettings.database
			});

			conn.on("error", (err) => {
				console.log("FAT FET FUCK::: TAG ==");

				if (err.code == 'PROTOCOL_CONNECTION_LOST') {
					console.log("FAT FET FUCK -- LOST --:::", err);
					this.connLost = true;
				}
			});

			if (openConnection) {
				conn.connect();
			}
		}
		catch (ex) {
			console.log("createConnection :: ERROR ::", ex);
		}

		return conn;
	}

	public executeTransaction(sql: string): Promise<IDbResult> {
		let scope = this;
		let subConn = this.createConnection();
		let result: IDbResult;
		let executeError: Error = null;

		function beginTransaction(): Promise<IOkPacket> {
			return new Promise((resolve, reject) => {
				subConn.query("START TRANSACTION", (error, result) => {
					if (!error) {
						resolve(result);
					}
					else {
						reject(error);
					}
				});
			});
		}

		function executeSql(sql: string): Promise<IDbResult> {
			return new Promise((resolve, reject) => {
				subConn.query(sql, (error, result, tableFields) => {
					scope.parseMysqlQueryResult(error, result, tableFields).then((res) => {
						resolve(res);
					}).catch((err) => {
						reject(err);
					});
				});
			});
		}

		function commit(): Promise<boolean> {
			return new Promise((resolve, reject) => {
				subConn.query("COMMIT", (error, result) => {
					console.log("error ::", error);
					console.log("result ::", result);
					if (!error) {
						resolve(result);
					}
					else {
						reject(error);
					}
				});
			});
		}

		function rollback(): Promise<boolean> {
			return new Promise((resolve, reject) => {
				subConn.query("ROLLBACK", (error, result) => {
					console.log("error ::", error);
					console.log("result ::", result);
					if (!error) {
						resolve(result);
					}
					else {
						reject(error);
					}
				});
			});
		}

		async function execute(): Promise<void> {
			let beginTransRes = await beginTransaction();

			try {
				result = await executeSql(sql);
				await commit();

			} catch(err) {
				let transError  = err != null ? err : new Error("SQL Execution failed");
				executeError = transError;
			}

			if (executeError != null || !result.success) {
				await rollback();
			}
		}

		return new Promise((resolve, reject) => {
			execute().then(() => {
				if (executeError != null) {
					reject(executeError)
				}
				else {
					resolve(result);
				}
			});
		});
	}

	/**
	 *
	 * @param {string} table
	 * @param {string} where
	 * @param {string} is
	 * @returns {Promise<number>}
	 */
	public countRows(table: string, where: string, is: string): Promise<number> {
		let countAlias = "count";
		return new Promise((resolve, reject) => {
			let query = `SELECT COUNT(*) AS ${countAlias} FROM ${table} WHERE ${where}${is}`;
			this.dbQuery(query).then(res => {
				let row = res.safeGetFirstRow();
				let count = row.getValAsNum(countAlias);
				resolve(count);

			}).catch(err => {
				resolve(-1);
			})
		});
	}

	public dbQuery(sql: string): Promise<IDbResult> {
		return new Promise((resolve, reject) => {
			let conn: Connection;

			try {
				conn = this.createConnection(true);
			}
			catch (ex) {
				console.log("Error Creating Connection");
				reject(ex);
			}

			conn.query(sql, (error, result, tableFields) => {
				if (error)  {
					console.log("dbQuery ERROR ::", error);
					if (error.fatal) {
						console.trace('fatal error: ' + error.message);
					}

					conn.end();
					reject(error);

				} else {
					return DbResultParser.parseQueryResult(error, result, tableFields).then((res) => {
						if (error) {
							console.log("FET ERROR ::", error);
							throw error;
						}

						return res;

					}).then((res) => {
						conn.end();
						resolve(res);

					}).catch((err) => {
						conn.end();
						reject(err);
					});
				}
			});
		});
	}
}
