"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var DUpdate = /** @class */ (function () {
    function DUpdate(table) {
        this.table = table;
    }
    return DUpdate;
}());
exports.DUpdate = DUpdate;
var DInsert = /** @class */ (function () {
    function DInsert(columns) {
        this.mySQLReplace = false;
        this.columns = columns;
    }
    return DInsert;
}());
exports.DInsert = DInsert;
var DWith = /** @class */ (function () {
    function DWith() {
        var data = [];
        for (var _i = 0; _i < arguments.length; _i++) {
            data[_i] = arguments[_i];
        }
        this.data = data;
    }
    return DWith;
}());
exports.DWith = DWith;
var DInto = /** @class */ (function () {
    function DInto(tableName) {
        this.tableName = tableName;
    }
    return DInto;
}());
exports.DInto = DInto;
var DSelect = /** @class */ (function () {
    function DSelect(column, alias) {
        if (alias === void 0) { alias = null; }
        this.column = column;
        this.haveAlias = alias != null;
    }
    return DSelect;
}());
exports.DSelect = DSelect;
var DSelectAll = /** @class */ (function () {
    function DSelectAll(column, alias) {
        if (alias === void 0) { alias = null; }
        this.column = column;
        this.haveAlias = alias != null;
    }
    return DSelectAll;
}());
exports.DSelectAll = DSelectAll;
var DSet = /** @class */ (function () {
    function DSet(column, value) {
        this.column = column;
        this.value = value;
    }
    return DSet;
}());
exports.DSet = DSet;
var DLeftJoin = /** @class */ (function () {
    function DLeftJoin(table, on) {
        this.table = table;
        this.on = on;
    }
    return DLeftJoin;
}());
exports.DLeftJoin = DLeftJoin;
var DFrom = /** @class */ (function () {
    function DFrom(table, alias) {
        if (alias === void 0) { alias = null; }
        this.table = table;
        this.alias = alias;
    }
    return DFrom;
}());
exports.DFrom = DFrom;
var DAnd = /** @class */ (function () {
    function DAnd(col, equals) {
        if (equals === void 0) { equals = null; }
        this.col = col;
        this.equals = equals;
    }
    return DAnd;
}());
exports.DAnd = DAnd;
var DWhere = /** @class */ (function () {
    function DWhere(that, equals) {
        if (equals === void 0) { equals = null; }
        this.that = that;
        this.equals = equals;
    }
    return DWhere;
}());
exports.DWhere = DWhere;
var DWhereExt = /** @class */ (function () {
    function DWhereExt(type, that, value1, value2) {
        if (value2 === void 0) { value2 = null; }
        this.type = type;
        this.that = that;
        this.value1 = value1;
        this.value2 = value2;
    }
    return DWhereExt;
}());
exports.DWhereExt = DWhereExt;
var DOrderBy = /** @class */ (function () {
    function DOrderBy(col) {
        this.col = col;
    }
    ;
    return DOrderBy;
}());
exports.DOrderBy = DOrderBy;
var DLimit = /** @class */ (function () {
    function DLimit(fromValue, toValue) {
        if (toValue === void 0) { toValue = null; }
        this.fromValue = fromValue;
        this.toValue = toValue;
    }
    ;
    return DLimit;
}());
exports.DLimit = DLimit;
