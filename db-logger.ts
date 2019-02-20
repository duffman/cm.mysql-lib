
import chalk from "chalk";

const log = console.log;

export class DBLogger {
	private static error = chalk.bold.red;
	private static warning = chalk.bold.yellow;

	public static logErrorMessage(errorMessage: string, error: Error = null) {
		if (error == null)
			log(this.error(errorMessage));
		else
			log(this.error(errorMessage), error);
	}
}
