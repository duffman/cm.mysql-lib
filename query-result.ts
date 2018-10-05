
import {SQLTableData} from "./sql-table-data";

export class QueryResult {
	constructor(
		public tableData: SQLTableData = null,
		public error: Error = null
	) {}
}