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

import {SQLTableDataRow} from "./sql-table-data-row";
import {SQLDataColumn} from "./sql-data-column";
import {QueryResult} from "./query-result";
import {resolvePtr} from "dns";

export interface ISqlDataModel {
	fieldTypes: { [name: string]: number; };
	dataRows: Array<SQLTableDataRow>;
	rowCount(): number;
	safeGetFirstRow(): SQLTableDataRow;
	getFirstRow(): SQLTableDataRow;
}

export class SQLTableData implements ISqlDataModel {
	public fieldTypes: { [name: string]: number; } = {};
	public dataRows: Array<SQLTableDataRow>;

	constructor() {
		this.dataRows = Array<SQLTableDataRow>();
	}

	public rowCount(): number {
		return this.dataRows != null ? this.dataRows.length : 0;
	}

	public parseFields(fields: any) {
		for (let index in fields) {
			let obj: any = fields[index];

			let name: string = "";
			let type: string = "";

			for (let prop in obj) {
				if (obj.hasOwnProperty(prop)) {
					if (prop === "name") name = obj[prop];
					if (prop === "type") type = obj[prop];
				}
			}

			this.fieldTypes[name] = Number(type);
		}
	}

	addRow(): SQLTableDataRow {
		let newRow = new SQLTableDataRow();
		this.dataRows.push(newRow);
		return newRow;
	}

	// Patrik, Dec 18 2017: Added in order to support Unit Test
	pushRow(row: SQLTableDataRow) {
		this.dataRows.push(row);
	}

	/**
	 * Returns the first row in result
	 * @param safe - if set to true a non null result is  guaranteed
	 * @returns {SQLTableDataRow}
	 */
	public safeGetFirstRow(): SQLTableDataRow {
		let result =  this.dataRows[0];

		if (result == null) {
			result = new SQLTableDataRow();
		}

		return result;
	}

	public getFirstRow(): SQLTableDataRow {
		return this.dataRows[0];
	}

	parseRowCollection(dataRows: any) {
		function parseRow(dataRow: any, dbDataRow: SQLTableDataRow) {
			for (let cell in dataRow) {
				if (dataRow.hasOwnProperty(cell)) {
					let rowObj = dataRow[cell];

					dbDataRow.columns.push(
						new SQLDataColumn(cell, rowObj)
					);
				}
			}
		}

		for (let i = 0; i < dataRows.length; i++) {
			let row = dataRows[i];
			let dbDataRow = this.addRow();
			parseRow(row, dbDataRow);
		}
	}

	public parseResultSet(dataRows: any, dataFields: any): Promise<SQLTableData> {
		//console.log("dataRows:", dataRows);
		//console.log("dataFields:", dataFields);

		return new Promise((resolve, reject) => {
			try {
				this.parseFields(dataFields);
				this.parseRowCollection(dataRows);

			} catch(e) {
				reject(e);
			}

			resolve(this);
		});
	}

	public toString(): string {
		let result = "";

		/*
		for (var r in this.rows) {
			for (var col in r.columns) {
				result += col.key + ": " + col.value + "\n";
			}
			result += "-----------------\n";
		}
		*/

		return result;
	}
}