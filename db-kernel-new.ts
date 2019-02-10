/**
 * Copyright (C) Patrik Forsberg <patrik.forsberg@coldmind.com> - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

import { Settings }               from "@app/zappy.app.settings";
import * as mysql                 from "mysql";
import { DataSheet }              from "./data-containers/data-sheet";
import { SQLTableData }           from "./sql-table-data";
import { IDbResult }              from "./db-result";
import { DbResult }               from "./db-result";
import { DbLogger }               from "./db-logger";
import { Connection }             from 'mysql';

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

export interface IQuerySheetCallback {
	(sheet: DataSheet);
}

export class DbKernel {
	private conn: Connection;
	private connLost: boolean = false;

	constructor (public dbHost: string = Settings.Database.dbHost,
				 public dbUser: string = Settings.Database.dbUser,
				 public dbPass: string = Settings.Database.dbPass,
				 public dbName: string = Settings.Database.dbName,
				 public tag: string = "NO_TAG") {

		this.conn = this.createConnection();

		this.conn.on("error", (err) => {
			console.log("FAT FET FUCK::: TAG ==", tag);

			if (err.code == 'PROTOCOL_CONNECTION_LOST') {
				console.log("FAT FET FUCK -- LOST --:::", err);
				this.connLost = true;
			}
		});
	}

	public getState(): DbState {
		let result: DbState = DbState.Unset;

		switch (this.conn.state) {
			case "disconnected":
		}

		return result;
	}

	public showDebugInfo(): void {
		/*
		if(connection.state === 'disconnected'){
			return respond(null, { status: 'fail', message: 'server down'});
		}
		*/
		console.log("Connection State ::", this.conn.state);
	}

	public getConnection(): Connection {
		return this.conn;
	}

	public createConnection(openConnection: boolean = true): Connection {
		try {
			if (this.connLost) {
				console.log("createConnection :: NOT PROCEEDING");
				return;
			}

			this.conn = mysql.createConnection({
				host: this.dbHost,
				user: this.dbUser,
				password: this.dbPass,
				database: this.dbName
			});

			if (openConnection) {
				this.conn.connect();
			}
		}
		catch (ex) {
			console.log("createConnection :: ERROR ::", ex);
		}

		return this.conn;
	}

	public static getInstance(): DbKernel {
		let dbManager = new DbKernel();
		dbManager.open();
		return dbManager;
	}

	public open() {
		this.conn.connect();
	}

	public close() {
		this.conn.end();
	}

	public query(sql: string, callback: IQuerySheetCallback): void {
		let dataSheet: DataSheet = new DataSheet();

		this.conn.connect();

		this.conn.query(sql, (error: any, result: any, fields: any) => {
			this.conn.end();
			if (error) throw error;

			dataSheet.parseFields(fields);
			callback(dataSheet);
		});
	}

	public static escape(value: string): string {
		if (value === null || value === undefined) {
			value = '';
		}

		value = value.replace('"', '\"');
		value = value.replace("'", '\"');
		return value;
	}

	private parseMysqlQueryResult(error, result, tableFields): Promise<IDbResult> {
		return new Promise((resolve, reject) => {
			let queryResult = new DbResult();

			if (error) {
				queryResult.success = false;
				queryResult.error = error;
				let customError = error;

				//error code 1292

				if (error.errno === 'ECONNREFUSED') {
					customError = new Error("ECONNREFUSED");
				}
				if (error.errno == 1062) {
					customError = new Error("DUP_ENTRY");
				} else {
					DbLogger.logErrorMessage("dbQuery :: Error ::", error.errno);
				}

				reject(customError);
				//resolve(queryResult);

			} else {
				queryResult.affectedRows = result.affectedRows;
				queryResult.lastInsertId = result.insertId;

				let data = new SQLTableData();
				data.parseResultSet(result, tableFields).then((res) => {
					queryResult.result = res;
					resolve(queryResult);
				}).catch((err) => {
					reject(err);
				});
			}
		});
	}

	public runInTransaction(sql: string): Promise<IDbResult> {
		let scope = this;
		let result: IDbResult;
		let executeError: Error = null;

		function beginTransaction(): Promise<IOkPacket> {
			return new Promise((resolve, reject) => {
				scope.conn.query("START TRANSACTION", (error, result) => {
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
				scope.conn.query(sql, (error, result, tableFields) => {
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
				scope.conn.query("COMMIT", (error, result) => {
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
				scope.conn.query("ROLLBACK", (error, result) => {
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

	returnResult(): Promise<IDbResult> {
		return new Promise((resolve, reject) => {
		});
	}

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
		if (this.connLost) {
			console.log("createConnection :: NOT PROCEEDING");
			return;
		}

		// Detta är nytt
		return new Promise((resolve, reject) => {
			this.conn.query(sql, (error, result, tableFields) => {
				if (error)  {
					console.log("dbQuery ERROR ::", error);
					if (error.fatal) {
						console.trace('fatal error: ' + error.message);
					}

					reject(error);

				} else {
					return this.parseMysqlQueryResult(error, result, tableFields).then((res) => {
						if (error) {
							console.log("FET ERROR ::", error);

						} else {
							resolve(res);
						}

					}).catch((err) => {
						reject(err);
					});
				}
			});
		});
	}

	public dbQuery2(sql: string): Promise<IDbResult> {
		let result: IDbResult;

		function parseResult(error, result, tableFields) {}

		async function execute(): Promise<void> {
			this.conn.query(sql, (error, result, tableFields) => {

				if (error)  {
					if (error.fatal) {
						console.trace('fatal error: ' + error.message);
					}

					//	reject(error);
				} else {
					return this.parseMysqlQueryResult(error, result, tableFields).then((res) => {
						if (error) {
							console.log("FET ERROR ::", error);
							result.setError(error);
						} else {
							//			resolve(res);
						}

					}).catch((err) => {
						result.e
						//		reject(err);
					});

				}
			});
		}

		return new Promise((resolve, reject) => {
			execute().then(() => {
				resolve(result);
			}).catch(err => {
				reject(err)
			});
		});
	}
}
/**
 * Copyright (c) Patrik Forsberg <patrik.forsberg@coldmind.com> - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */
