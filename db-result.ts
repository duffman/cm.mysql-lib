/**
 * Copyright (c) Patrik Forsberg <patrik.forsberg@coldmind.com> - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 * September 2018
 */

import { IConnection }            from "mysql";
import { ISqlDataModel }          from "./sql-table-data";
import { SQLTableData }           from "./sql-table-data";
import { SQLTableDataRow }        from "./sql-table-data-row";
import { isUndefined }            from "util";

export interface IDbResult {
	success: boolean;
	lastInsertId: number;
	affectedRows: number;
	result: ISqlDataModel;
	rawObj: {};
	safeGetFirstRow(): SQLTableDataRow;
	haveAny(): boolean;
}

export class DbResult implements IDbResult {
	public success: boolean = true;
	public error: any = null;
	public lastInsertId: number = 0;
	public affectedRows: number;
	public rawObj: {};

	constructor(
		public result: ISqlDataModel = null
	){
		this.setRawObj();
	}

	public errorMessage(): string {
		return "";
	}

	public haveAny(): boolean {
		return this.result.rowCount() > 0;
	}


	/**
	 * Store a raw JavaScript object representation of the data
	 * TODO: Clean up this hack!!
	 */
	private setRawObj(): void {
		if (this.result == null || this.result[0] == null)
			return;

		let obj = this.result[0];
		let empty = {};

		for (let key in obj) {
			if (obj.hasOwnProperty(key) ) {
				let camelKey = key; //PascalSnake.toPascalCase(key, true);
				empty[camelKey] = obj[key];
			}
		}

		this.rawObj = empty;
	}

	/**
	 * Always return a SQLTableDataRow
	 * If a SQLTableData result is present containing
	 * one or more rows, the first row will be returned
	 * otherwize a new SQLTableDataRow will be created.
	 */
	public safeGetFirstRow(): SQLTableDataRow {
		let tableDataRow: SQLTableDataRow;
		let isObj = this.result != null && !isUndefined(this.result);
		let isTableData = this.result instanceof SQLTableData;

		if (isObj && isTableData && this.result.dataRows.length > 0) {
			tableDataRow = this.result.getFirstRow()
		} else {
			tableDataRow = new SQLTableDataRow();
		}

		return tableDataRow;
	}
}