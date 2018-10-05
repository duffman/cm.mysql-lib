/**
 * COLDMIND LTD ("COMPANY") CONFIDENTIAL
 * Unpublished Copyright (c) 2015-2017 COLDMIND LTD, All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains the property of COMPANY. The intellectual and technical concepts contained
 * herein are proprietary to COMPANY and may be covered by U.S. and Foreign Patents, patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material is strictly forbidden unless prior written permission is obtained
 * from COMPANY.  Access to the source code contained herein is hereby forbidden to anyone except current COMPANY employees, managers or contractors who have executed
 * Confidentiality and Non-disclosure agreements explicitly covering such access.
 *
 * The copyright notice above does not evidence any actual or intended publication or disclosure  of  this source code, which includes
 * information that is confidential and/or proprietary, and is a trade secret, of  COMPANY.   ANY REPRODUCTION, MODIFICATION, DISTRIBUTION, PUBLIC  PERFORMANCE,
 * OR PUBLIC DISPLAY OF OR THROUGH USE  OF THIS  SOURCE CODE  WITHOUT  THE EXPRESS WRITTEN CONSENT OF COMPANY IS STRICTLY PROHIBITED, AND IN VIOLATION OF APPLICABLE
 * LAWS AND INTERNATIONAL TREATIES.  THE RECEIPT OR POSSESSION OF  THIS SOURCE CODE AND/OR RELATED INFORMATION DOES NOT CONVEY OR IMPLY ANY RIGHTS
 * TO REPRODUCE, DISCLOSE OR DISTRIBUTE ITS CONTENTS, OR TO MANUFACTURE, USE, OR SELL ANYTHING THAT IT  MAY DESCRIBE, IN WHOLE OR IN PART.
 *
 * Created by Patrik Forsberg on 2017
 */

import { isString } from "util";
import { isNumber } from "util";
import { isDate }   from "util";
import {
	DAnd,
	DFrom,
	DLeftJoin,
	DLimit,
	DOrderBy,
	DSelect,
	DSelectAll,
	DSet,
	DWhere,
	DWhereExt,
	DInsert,
	DUpdate, DInto, DWith
} from "./records";

// Keywords
const DB_INSERT          = "INSERT";
const DB_MYSQL_REPLACE   = "REPLACE";
const DB_SELECT          = "SELECT";
const DB_UPDATE          = "UPDATE";
const DB_DELETE          = "DELETE";
const DB_FROM            = "FROM";
const DB_WHERE           = "WHERE";
const DB_SET             = "SET";
const DB_DROP            = "DROP";

export interface IDRecord {}

export enum WhereType {
	Between,
	Or,
	In,
}

export enum DataType {
	VarChar,
	Boolean,
	Int,
	Date
}

function prepMySQLDate(dateObj: Date): string {
	dateObj.setHours(dateObj.getHours() - 2);
	return dateObj.toISOString().slice(0, 19).replace('T', ' ');
}

export class DataColumn {
	dataType: DataType;
	name: string;
	length: number;

	constructor(public value: any) {}
}

/**
 * Simple Active Record implementation
 * Note: This does not add any intelligens, stupid behaviour such
 * as calling an SELECT after a SET, broken SQL will remain broken :)
 */
export class DynSQL {
	dbName: string;
	private records: Array<IDRecord>;

	constructor(dbName: string = "") {
		this.dbName = dbName;
		this.records = new Array<IDRecord>();
	}

	/**
	 * For this sucker I actually performed a series of
	 * performance benchmarks, this is (at least for this
	 * app) the fastest and the most
	 */
	public clear() {
		this.records.length = 0;
	}

	/**
	 * Returns the previous record from a given
	 * record in the record array
	 * @param {IDRecord} record
	 * @returns {IDRecord}
	 */
	private getPreviousRecord(record: IDRecord): IDRecord {
		let result: IDRecord = null;
		let index = this.records.indexOf(record);
		if (index > -1 && index -1 > 0) {
			result = this.records[index] as IDRecord;
		}
		return result;
	}

	public selectAll(...elements: Array<string>) {
		for (let item in elements) {
			let name = elements[item];
			this.records.push(new DSelectAll(name));
		}
		return this;
	}


	public select(...elements: Array<string>): DynSQL {
		for (let item in elements) {
			let name = elements[item];
			this.records.push(new DSelect(name));
		}
		return this;
	}

	public update(table: string): DynSQL {
		this.records.push(new DUpdate(table));
		return this;
	}

	public insert(...columns: Array<string>): DynSQL {
		this.records.push(new DInsert(columns));
		return this;
	}

	public with(...data: Array<any>): DynSQL {
		this.records.push(new DWith(data));
		return this;
	}

	public into(tableName: string): DynSQL {
		this.records.push(new DInto(tableName));
		return this;
	}

	public set(column: string, value: any, escape: boolean = true): DynSQL {
		this.records.push(new DSet(column, value));
		return this;
	}

	public leftJoin(table: string, on: string): DynSQL {
		this.records.push(new DLeftJoin(table, on));
		return this;
	}

	public selectAs(fromTable: string, alias: string = null): DynSQL {
		this.records.push(new DSelect(fromTable, alias));
		return this;
	}

	public from(table: string, alias: string = null): DynSQL {
		let rec = new DFrom(table, alias);
		this.records.push(rec);
		return this;
	}

	private prepValue(value: any): string {
		if (isString(value)) {
			value = "'" + value + "'";
		}
		else if (isNumber(value)) {
			value = String(value);
		}
		else if (isDate(value)) {
			value = prepMySQLDate(value);
		}

		return value;
	}

	/**
	 * Adds a Where record to the active record stack
	 * @param thisElem
	 * @param elemIs
	 * @param escapeValue - set this to true when handling user inputted values, false when like "lucas.arts=rulez.row"
	 * @returns {DynSQL}
	 */
	public where(thisElem: string, elemIs: any = null, escapeValue: boolean = true) {
		let equalValue = escapeValue ? this.prepValue(elemIs) : elemIs;
		let rec = new DWhere(thisElem, equalValue);
		this.records.push(rec);

		return this;
	}

	// withintrig
	public whereBetween(value: any, rangeStart: any, rangeEnd: any): DynSQL {
		this.prepValue(value);

		let rec = new DWhereExt(WhereType.Between, value, rangeStart, rangeEnd);
		this.records.push(rec);
		return this;
	}

	public orderBy(col: string): DynSQL {
		let rec = new DOrderBy(col);
		this.records.push(rec);
		return this;
	}

	public orderByRandom(): DynSQL {
		let rec = new DOrderBy("RAND()");
		this.records.push(rec);
		return this;
	}

	public and(col: string, equals: any = null) {
		let rec = new DAnd(col, equals);
		this.records.push(rec);
		return this;
	}

	public limitBy(fromValue: number, toValue: number = null): DynSQL {
		let rec = new DLimit(fromValue, toValue);
		this.records.push(rec);
		return this;
	}

	/**
	 * Checks whether a given record position contains a
	 * given instance type.
	 * @param recordNum
	 * @param recordInstanceType
	 */
	public isExpectedRecord(recordNum: number, recordInstanceType: any): boolean {
		let isExpected: boolean = false;
		let inRange = (recordNum <= this.records.length && recordNum >= 0);

		if (inRange && (this.records[recordNum] instanceof recordInstanceType)) {
			isExpected = true;
		}

		return isExpected;
	}

	public toSQL(): string {
		let sql: string = "";

		/**
		 * Iterate the array on loopback for each type, that´s the most system
		 * efficient and readable, don´t get confused by compiler masturbations
		 * and smart array functions, they will boil down to something much
		 * worse if you look behind the curtain.
		 */

		sql = this.parseInsert(sql);
		sql = this.parseSelect(sql);
		sql = this.parseSelectAll(sql);
		sql = this.parseUpdate(sql);
		sql = this.parseSet(sql);
		sql = this.parseFrom(sql);
		sql = this.parseLeftJoin(sql);
		sql = this.parseWhere(sql);
		sql = this.parseAnd(sql);
		sql = this.parseOrderBy(sql);
		sql = this.parseLimit(sql);

		return sql;
	}

	////////////////////////////////////////
	// SELECT

	parseInsert(sql: string): string {
		let localCounter = 0;

		for (let i = 0; i < this.records.length; i++) {
			let record = this.records[i];

			if (record instanceof DInsert) {
				const dRec: DInsert = record as DInsert;
				let type = dRec.mySQLReplace ? DB_MYSQL_REPLACE : DB_INSERT;

				sql = "INSERT (";

				for (let col = 0; col < dRec.columns.length; col++) {
 					sql += dRec.columns[col];

					if (col < dRec.columns.length-1) {
						sql += ", ";
					}
				}

				sql += ") VALAAUES (";

				let withRec = this.records[i+1] as DWith;

				for (let val = 0; val < withRec.data.length; val++) {

				}

				localCounter++;
			}
		} // end for

		return sql;
	}

	////////////////////////////////////////
	// SELECT

	parseSelect(sql: string): string {
		let localCounter = 0;

		for (let i = 0; i < this.records.length; i++) {
			let record = this.records[i];

			if (record instanceof DSelect) {
				const dRec = record as DSelect;

				if (localCounter == 0) {
					sql += "SELECT"
				} else {
					sql += ",";
				}

				sql += " " + dRec.column;

				localCounter++;
			}
		} // end for

		return sql;
	} // parseSelect

	parseSelectAll(sql: string): string {
		let localCounter = 0;

		for (let i = 0; i < this.records.length; i++) {

			const record = this.records[i];

			if (record instanceof DSelectAll) {
				const dRec = record as DSelectAll;

				if (localCounter == 0) {
					sql += "SELECT"
				} else {
					sql += ",";
				}

				sql += " " + dRec.column + ".*";

				localCounter++;
			}
		} // end for

		return sql;
	} // parseSelect


	////////////////////////////////////////
	// FROM

	parseFrom(sql: string): string {
		let localCounter = 0;

		for (let i = 0; i < this.records.length; i++) {
			const record = this.records[i];

			if (record instanceof DFrom) {
				const rec = record as DFrom;

				if (localCounter == 0) {
					sql += " FROM"
				} else {
					sql += ",";
				}

				sql += " " + rec.table;

				if (rec.alias != null) {
					sql += " AS " + rec.alias;
				}

				localCounter++;
			}
		}
		return sql;
	} // parseFrom

	////////////////////////////////////////
	// UPDATE

	parseUpdate(sql: string): string {
		for (let i = 0; i < this.records.length; i++) {
			let record = this.records[i];

			if (record instanceof DUpdate) {
				const rec = record as DUpdate;
				sql += "UPDATE " + rec.table;
				break;
			}
		}
		return sql;
	}

	////////////////////////////////////////
	// SET

	parseSet(sql: string): string {
		let rec: DSet;

		let localCounter = 0;

		for (let i = 0; i < this.records.length; i++) {
			let record = this.records[i];

			if (record instanceof DSet) {
				rec = record as DSet;
				if (localCounter == 0) {
					sql += " SET"
				} else {
					sql += " ,";
 				}

 				let val = rec.escape ? this.prepValue(rec.value) : rec.value;
				sql += " " + rec.column + "='" + val + "'";

				localCounter++;
			}
		} // end for

		return sql;
	} // parseSet

	////////////////////////////////////////
	// LEFT JOIN

	parseLeftJoin(sql: string): string {
		const localCounter = 0;

		for (let i = 0; i < this.records.length; i++) {
			const record = this.records[i];

			if (record instanceof DLeftJoin) {
				const rec = record as DLeftJoin;

				sql += " LEFT JOIN "
				+ rec.table + " ON "
				+ rec.on
			}
		}
		return sql;
	} // parseLeftJoin

	////////////////////////////////////////
	// WHERE
	isWhereRecord(record: any) {
		return (record instanceof DWhere || record instanceof DWhereExt);
	}

	parseWhere(sql: string): string {
		let firstIteration = true;

		for (let i = 0; i < this.records.length; i++) {
			let record = this.records[i];
			if (!this.isWhereRecord(record)) continue;

			if (firstIteration) {
				sql += " WHERE";

			} else {
				sql += " AND";

			}

			let rec: any;

			if (record instanceof DWhereExt) {
				rec = record as DWhereExt;
				sql += " " + rec.that + " BETWEEN '" + this.prepValue(rec.value1) + "' AND " + this.prepValue(rec.value2);

			}
			else if (record instanceof DWhere) {
				rec = record as DWhere;
				sql += " " + rec.that;

				if (rec.equals != null)
					sql += "="  + rec.equals;
			}

			firstIteration = false;
		} // end for

		return sql;
	} // parseWhere

	////////////////////////////////////////
	// And

	parseAnd(sql: string): string {
		for (let i = 0; i < this.records.length; i++) {
			let record = this.records[i];

			if (record instanceof DAnd) {
				let rec = record as DAnd;
				sql += " AND " + rec.col;
				sql += " = '" + this.prepValue(rec.equals) + "'";

				break;
			}
		}
		return sql;
	}

	////////////////////////////////////////
	// Order

	parseOrderBy(sql: string): string {
		for (let i = 0; i < this.records.length; i++) {
			let record = this.records[i];

			if (record instanceof DOrderBy) {
				let rec = record as DOrderBy;
				sql += " ORDER BY " + rec.col;

				break;
			}
		}
		return sql;

	} // end parseOrderBy

	////////////////////////////////////////
	// Limit

	parseLimit(sql: string): string {
		for (let i = 0; i < this.records.length; i++) {
			const record = this.records[i];

			if (record instanceof DLimit) {
				const rec = record as DLimit;
				sql += " LIMIT " + rec.fromValue;

				if (rec.toValue != null) {
					sql += ", " + rec.toValue;
				}

				break;
			}
		}
		return sql;
	} // end parseLimit
}
