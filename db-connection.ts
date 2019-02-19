/**
 * Copyright (c) Patrik Forsberg <patrik.forsberg@coldmind.com> - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

import { Connection }             from 'mysql';
import { IConnectionSettings }    from './db-kernel';

/**
 Wrapped MySQL Connection with auto connect
 **/

export interface IDbConnection {
	configure(settins: IConnectionSettings): void;
}

export class DbConnection implements IDbConnection {
	configure(settins: IConnectionSettings): void {
		throw new Error("Method not implemented.");
	}
}