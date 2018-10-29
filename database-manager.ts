/**
 * Copyright (C) Patrik Forsberg <patrik.forsberg@coldmind.com> - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * 2017-2018
 */

import * as mysql                 from "mysql";
import { DataSheet }              from "./data-sheet";
import { SQLTableData }           from "./sql-table-data";
import { IDbResult }              from "./db-result";
import { DbResult }               from "./db-result";
import { isNullOrUndefined }      from "util";
import { Global }                 from "@app/global";
import { Logger }                 from "@global/logger";

const log = console.log;


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

export interface IQuerySheetCallback {
	(sheet: DataSheet);
}

export class DbManager {
	connection: any;

	constructor (public dbHost: string = Global.Settings.Database.dbHost,
				 public dbUser: string = Global.Settings.Database.dbUser,
				 public dbPass: string = Global.Settings.Database.dbPass,
				 public dbName: string = Global.Settings.Database.dbName) {

		this.connection = mysql.createConnection({
			host: dbHost,
			user: dbUser,
			password: dbPass,
			database: dbName
		});
	}

	public static getInstance(): DbManager {
		let dbManager = new DbManager();
		dbManager.open();
		return dbManager;
	}

	public open() {
		this.connection.connect();
	}

	public close() {
		this.connection.end();
	}

	public query(sql: string, callback: IQuerySheetCallback): void {
		let dataSheet: DataSheet = new DataSheet();

		this.connection.connect();

		this.connection.query(sql, (error: any, result: any, fields: any) => {
			this.connection.end();
			if (error) throw error;

			dataSheet.parseFields(fields);
			callback(dataSheet);
		});
	}

	public static escape(value: string): string {
		if (isNullOrUndefined(value))
			value = '';

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

				if (error.errno == 1062) {
					//log("** Duplicate entry")
				} else {
					Logger.logErrorMessage("dbQuery :: Error ::", error.errno);
				}

				//reject(error);
				resolve(queryResult);
				return;
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
				scope.connection.query("START TRANSACTION", (error, result) => {
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
				scope.connection.query(sql, (error, result, tableFields) => {
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
				scope.connection.query("COMMIT", (error, result) => {
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
				scope.connection.query("ROLLBACK", (error, result) => {
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

	public dbQuery(sql: string): Promise<IDbResult> {
		return new Promise((resolve, reject) => {
			this.connection.query(sql, (error, result, tableFields) => {
				this.parseMysqlQueryResult(error, result, tableFields).then((res) => {
					resolve(res);
				}).catch((err) => {
					reject(err);
				});
			});
		});
	}
}