

import {DataColumn, IDRecord, WhereType} from "./dynsql";

export class DUpdate implements IDRecord {
	constructor(public table: string) {}
}

export class DInsert implements IDRecord {
	public mySQLReplace: boolean = false;
	public columns: Array<string>;
	constructor(columns: Array<string>) {
		this.columns = columns;
	}
}

export class DWith implements IDRecord {
	public data: Array<string>
	constructor(...data: Array<any>) {
		this.data = data;
	}
}

export class DInto implements IDRecord {
	constructor(public tableName: string) {}
}

export class DSelect implements IDRecord {
	public haveAlias: boolean;

	constructor(public column: string, alias: string = null) {
		this.haveAlias = alias != null;
	}
}

export class DSelectAll implements IDRecord {
	public haveAlias: boolean;

	constructor(public column: string, alias: string = null) {
		this.haveAlias = alias != null;
	}
}

export class DSet implements IDRecord {
	constructor(public column: string, public value, public escape: boolean = true) {}
}

export class DLeftJoin implements IDRecord {
	constructor(public table: string, public on: string) {}
}

export class DFrom implements IDRecord {
	constructor(public table: string, public alias: string = null) {}
}

export class DAnd implements IDRecord {
	constructor(public col: string, public equals: any = null) {}
}

export class DWhere implements IDRecord {
	constructor(public that: string, public equals: string = null) {}
}

export class DWhereExt implements IDRecord {
	constructor(
		public type: WhereType,
		public that: any,
		public value1: any,
		public value2: any = null
	) {}
}

export class DOrderBy implements IDRecord {
	constructor(public col: string) {};
}

export class DLimit implements IDRecord {
	constructor(public fromValue: number, public toValue: number = null) {};
}