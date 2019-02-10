/**
 * Copyright (c) Patrik Forsberg <patrik.forsberg@coldmind.com> - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

export class DbVarUtils {
	public static isDefined(value: any): boolean {
		return value === null || value === undefined;
	}
}
