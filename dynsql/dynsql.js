"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
var util_1 = require("util");
var util_2 = require("util");
var util_3 = require("util");
var records_1 = require("./records");
// Keywords
var DB_INSERT = "INSERT";
var DB_MYSQL_REPLACE = "REPLACE";
var DB_SELECT = "SELECT";
var DB_UPDATE = "UPDATE";
var DB_DELETE = "DELETE";
var DB_FROM = "FROM";
var DB_WHERE = "WHERE";
var DB_SET = "SET";
var DB_DROP = "DROP";
var WhereType;
(function (WhereType) {
    WhereType[WhereType["Between"] = 0] = "Between";
    WhereType[WhereType["Or"] = 1] = "Or";
    WhereType[WhereType["In"] = 2] = "In";
})(WhereType = exports.WhereType || (exports.WhereType = {}));
var DataType;
(function (DataType) {
    DataType[DataType["VarChar"] = 0] = "VarChar";
    DataType[DataType["Boolean"] = 1] = "Boolean";
    DataType[DataType["Int"] = 2] = "Int";
    DataType[DataType["Date"] = 3] = "Date";
})(DataType = exports.DataType || (exports.DataType = {}));
function prepMySQLDate(dateObj) {
    dateObj.setHours(dateObj.getHours() - 2);
    return dateObj.toISOString().slice(0, 19).replace('T', ' ');
}
var DataColumn = /** @class */ (function () {
    function DataColumn(value) {
        this.value = value;
    }
    return DataColumn;
}());
exports.DataColumn = DataColumn;
/**
 * Simple Active Record implementation
 * Note: This does not add any intelligens, stupid behaviour such
 * as calling an SELECT after a SET, broken SQL will remain broken :)
 */
var DynSQL = /** @class */ (function () {
    function DynSQL(dbName) {
        if (dbName === void 0) { dbName = ""; }
        this.dbName = dbName;
        this.records = new Array();
    }
    /**
     * For this sucker I actually performed a series of
     * performance benchmarks, this is (at least for this
     * app) the fastest and the most
     */
    DynSQL.prototype.clear = function () {
        this.records.length = 0;
    };
    /**
     * Returns the previous record from a given
     * record in the record array
     * @param {IDRecord} record
     * @returns {IDRecord}
     */
    DynSQL.prototype.getPreviousRecord = function (record) {
        var result = null;
        var index = this.records.indexOf(record);
        if (index > -1 && index - 1 > 0) {
            result = this.records[index];
        }
        return result;
    };
    DynSQL.prototype.selectAll = function () {
        var elements = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            elements[_i] = arguments[_i];
        }
        for (var item in elements) {
            var name_1 = elements[item];
            this.records.push(new records_1.DSelectAll(name_1));
        }
        return this;
    };
    DynSQL.prototype.select = function () {
        var elements = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            elements[_i] = arguments[_i];
        }
        for (var item in elements) {
            var name_2 = elements[item];
            this.records.push(new records_1.DSelect(name_2));
        }
        return this;
    };
    DynSQL.prototype.update = function (table) {
        this.records.push(new records_1.DUpdate(table));
        return this;
    };
    DynSQL.prototype.insert = function () {
        var columns = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            columns[_i] = arguments[_i];
        }
        this.records.push(new records_1.DInsert(columns));
        return this;
    };
    DynSQL.prototype.with = function () {
        var data = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            data[_i] = arguments[_i];
        }
        this.records.push(new records_1.DWith(data));
        return this;
    };
    DynSQL.prototype.into = function (tableName) {
        this.records.push(new records_1.DInto(tableName));
        return this;
    };
    DynSQL.prototype.set = function (column, value) {
        this.records.push(new records_1.DSet(column, value));
        return this;
    };
    DynSQL.prototype.leftJoin = function (table, on) {
        this.records.push(new records_1.DLeftJoin(table, on));
        return this;
    };
    DynSQL.prototype.selectAs = function (fromTable, alias) {
        if (alias === void 0) { alias = null; }
        this.records.push(new records_1.DSelect(fromTable, alias));
        return this;
    };
    DynSQL.prototype.from = function (table, alias) {
        if (alias === void 0) { alias = null; }
        var rec = new records_1.DFrom(table, alias);
        this.records.push(rec);
        return this;
    };
    DynSQL.prototype.prepValue = function (value) {
        if (util_1.isString(value)) {
            value = "'" + value + "'";
        }
        else if (util_2.isNumber(value)) {
            value = String(value);
        }
        else if (util_3.isDate(value)) {
            value = prepMySQLDate(value);
        }
        return value;
    };
    /**
     * Adds a Where record to the active record stack
     * @param thisElem
     * @param elemIs
     * @param escapeValue - set this to true when handling user inputted values, false when like "lucas.arts=rulez.row"
     * @returns {DynSQL}
     */
    DynSQL.prototype.where = function (thisElem, elemIs, escapeValue) {
        if (elemIs === void 0) { elemIs = null; }
        if (escapeValue === void 0) { escapeValue = true; }
        var equalValue = escapeValue ? this.prepValue(elemIs) : elemIs;
        var rec = new records_1.DWhere(thisElem, equalValue);
        this.records.push(rec);
        return this;
    };
    // withintrig
    DynSQL.prototype.whereBetween = function (value, rangeStart, rangeEnd) {
        this.prepValue(value);
        var rec = new records_1.DWhereExt(WhereType.Between, value, rangeStart, rangeEnd);
        this.records.push(rec);
        return this;
    };
    DynSQL.prototype.orderBy = function (col) {
        var rec = new records_1.DOrderBy(col);
        this.records.push(rec);
        return this;
    };
    DynSQL.prototype.orderByRandom = function () {
        var rec = new records_1.DOrderBy("RAND()");
        this.records.push(rec);
        return this;
    };
    DynSQL.prototype.and = function (col, equals) {
        if (equals === void 0) { equals = null; }
        var rec = new records_1.DAnd(col, equals);
        this.records.push(rec);
        return this;
    };
    DynSQL.prototype.limitBy = function (fromValue, toValue) {
        if (toValue === void 0) { toValue = null; }
        var rec = new records_1.DLimit(fromValue, toValue);
        this.records.push(rec);
        return this;
    };
    /**
     * Checks whether a given record position contains a
     * given instance type.
     * @param recordNum
     * @param recordInstanceType
     */
    DynSQL.prototype.isExpectedRecord = function (recordNum, recordInstanceType) {
        var isExpected = false;
        var inRange = (recordNum <= this.records.length && recordNum >= 0);
        if (inRange && (this.records[recordNum] instanceof recordInstanceType)) {
            isExpected = true;
        }
        return isExpected;
    };
    DynSQL.prototype.toSQL = function () {
        var sql = "";
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
    };
    ////////////////////////////////////////
    // SELECT
    DynSQL.prototype.parseInsert = function (sql) {
        var localCounter = 0;
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DInsert) {
                var dRec = record;
                var type = dRec.mySQLReplace ? DB_MYSQL_REPLACE : DB_INSERT;
                sql = "INSERT (";
                for (var col = 0; col < dRec.columns.length; col++) {
                    sql += dRec.columns[col];
                    if (col < dRec.columns.length - 1) {
                        sql += ", ";
                    }
                }
                sql += ") VALAAUES (";
                var withRec = this.records[i + 1];
                for (var val = 0; val < withRec.data.length; val++) {
                }
                localCounter++;
            }
        } // end for
        return sql;
    };
    ////////////////////////////////////////
    // SELECT
    DynSQL.prototype.parseSelect = function (sql) {
        var localCounter = 0;
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DSelect) {
                var dRec = record;
                if (localCounter == 0) {
                    sql += "SELECT";
                }
                else {
                    sql += ",";
                }
                sql += " " + dRec.column;
                localCounter++;
            }
        } // end for
        return sql;
    }; // parseSelect
    DynSQL.prototype.parseSelectAll = function (sql) {
        var localCounter = 0;
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DSelectAll) {
                var dRec = record;
                if (localCounter == 0) {
                    sql += "SELECT";
                }
                else {
                    sql += ",";
                }
                sql += " " + dRec.column + ".*";
                localCounter++;
            }
        } // end for
        return sql;
    }; // parseSelect
    ////////////////////////////////////////
    // FROM
    DynSQL.prototype.parseFrom = function (sql) {
        var localCounter = 0;
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DFrom) {
                var rec = record;
                if (localCounter == 0) {
                    sql += " FROM";
                }
                else {
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
    }; // parseFrom
    ////////////////////////////////////////
    // UPDATE
    DynSQL.prototype.parseUpdate = function (sql) {
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DUpdate) {
                var rec = record;
                sql += "UPDATE " + rec.table;
                break;
            }
        }
        return sql;
    };
    ////////////////////////////////////////
    // SET
    DynSQL.prototype.parseSet = function (sql) {
        var rec;
        var localCounter = 0;
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DSet) {
                rec = record;
                if (localCounter == 0) {
                    sql += " SET";
                }
                else {
                    sql += " ,";
                }
                sql += " " + rec.column + "='" + this.prepValue(rec.value) + "'";
                localCounter++;
            }
        } // end for
        return sql;
    }; // parseSet
    ////////////////////////////////////////
    // LEFT JOIN
    DynSQL.prototype.parseLeftJoin = function (sql) {
        var localCounter = 0;
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DLeftJoin) {
                var rec = record;
                sql += " LEFT JOIN "
                    + rec.table + " ON "
                    + rec.on;
            }
        }
        return sql;
    }; // parseLeftJoin
    ////////////////////////////////////////
    // WHERE
    DynSQL.prototype.isWhereRecord = function (record) {
        return (record instanceof records_1.DWhere || record instanceof records_1.DWhereExt);
    };
    DynSQL.prototype.parseWhere = function (sql) {
        var firstIteration = true;
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (!this.isWhereRecord(record))
                continue;
            if (firstIteration) {
                sql += " WHERE";
            }
            else {
                sql += " AND";
            }
            var rec = void 0;
            if (record instanceof records_1.DWhereExt) {
                rec = record;
                sql += " " + rec.that + " BETWEEN '" + this.prepValue(rec.value1) + "' AND " + this.prepValue(rec.value2);
            }
            else if (record instanceof records_1.DWhere) {
                rec = record;
                sql += " " + rec.that;
                if (rec.equals != null)
                    sql += "=" + rec.equals;
            }
            firstIteration = false;
        } // end for
        return sql;
    }; // parseWhere
    ////////////////////////////////////////
    // And
    DynSQL.prototype.parseAnd = function (sql) {
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DAnd) {
                var rec = record;
                sql += " AND " + rec.col;
                sql += " = '" + this.prepValue(rec.equals) + "'";
                break;
            }
        }
        return sql;
    };
    ////////////////////////////////////////
    // Order
    DynSQL.prototype.parseOrderBy = function (sql) {
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DOrderBy) {
                var rec = record;
                sql += " ORDER BY " + rec.col;
                break;
            }
        }
        return sql;
    }; // end parseOrderBy
    ////////////////////////////////////////
    // Limit
    DynSQL.prototype.parseLimit = function (sql) {
        for (var i = 0; i < this.records.length; i++) {
            var record = this.records[i];
            if (record instanceof records_1.DLimit) {
                var rec = record;
                sql += " LIMIT " + rec.fromValue;
                if (rec.toValue != null) {
                    sql += ", " + rec.toValue;
                }
                break;
            }
        }
        return sql;
    }; // end parseLimit
    return DynSQL;
}());
exports.DynSQL = DynSQL;
