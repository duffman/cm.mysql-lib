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

import { isNullOrUndefined }      from "util";
import {SQLDataColumn}            from "./sql-data-column";
import {DataPoint}                from "./data-point";

export class SQLTableDataRow {
	public obj: any;
	public columns: Array<SQLDataColumn>;

	constructor(obj?: any) {
		this.columns = new Array<SQLDataColumn>();
		if (!isNullOrUndefined(obj)) {
			this.parseData(obj);
		}
	}

	public parseData(obj: any): void {
		for (let key in obj) {
			if (obj.hasOwnProperty(key) ) {
				this.columns.push(new SQLDataColumn(key, obj[key]));
			}
		}
	}

	public getColumn(key: string): SQLDataColumn {
		for (let i = 0; i < this.columns.length; i++) {
			let column = this.columns[i];
			if (column.name == key) {
				return column;
			}
		}
		return null;
	}

	public count() {
		return this.columns.length;
	}

	public emptyValue(key: string): void {
		let column = this.getColumn(key);
		if (column != null) column.value = null;
	}

	public getValAsStr(key: string): string {
		let column = this.getColumn(key);
		return column != null ? column.value : null;
	}

	public getValAsCVPoint(key: string): DataPoint {
		let column = this.getColumn(key);
		let res = column != null ? column.value : null;

		return new DataPoint(0,0);
	}

	public getValAsNum(key: string): number {
		let value = this.getValAsStr(key);
		if (value != null) {
			return Number(value);
		}
		return -1;
	}

	/**
	 * Makes an optimistic attempt to parse a JS date from given string
 	 * @param {string} key
	 * @returns {Date}
	 *
	public getValAsDate(key: string): Date {
		return res;
	}
	*/

	public getValAsInt(key: string): number {
		return this.getValAsNum(key);
	}

	public toJson() {
		let data = this.columns != null ? this.columns : "NULL";
		return JSON.stringify(data);
	}
}