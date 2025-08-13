const {
	EvidenceType,
	TypeFidelity,
	asyncIterableToBatchedAsyncGenerator,
	cleanQuery,
	exhaustStream
} = require('@evidence-dev/db-commons');
const mssql = require('mssql');

/**
 * Retry an async function with exponential backoff
 * @param {Function} fn - The async function to retry
 * @param {Object} options - Retry options
 * @param {number} options.retries - Max number of retries (default: 3)
 * @param {number} options.delay - Initial delay in ms (default: 1000)
 * @param {number} options.backoffFactor - Multiplier for exponential backoff (default: 2)
 * @param {string[]} options.retryableErrors - Error messages or codes to retry on
 * @returns {Promise<any>} - Result of the function
 */
async function retry(fn, { retries = 3, delay = 1000, backoffFactor = 2, retryableErrors = [] } = {}) {
	let lastError;
	for (let attempt = 1; attempt <= retries + 1; attempt++) {
		try {
			return await fn();
		} catch (err) {
			// Normalise error
			if (!(err instanceof Error)) {
				if (typeof err === 'string') {
					err = new Error(err);
				} else if (err && typeof err.toString === 'function') {
					err = new Error(err.toString());
				} else {
					err = new Error('Unknown error');
				}
			}

			lastError = err;
			const errorMessage = err.message || err.toString();

			// Check if the error is retryable
			const isRetryable = retryableErrors.some(retryError =>
				errorMessage.includes(retryError)
			);

			if (!isRetryable || attempt === retries + 1) {
				throw lastError; // No more retries or non-retryable error
			}

			// Calculate delay with exponential backoff
			const waitTime = delay * Math.pow(backoffFactor, attempt - 1);
			console.log(`Attempt ${attempt} failed: ${errorMessage}. Retrying in ${waitTime}ms...`);
			await new Promise(resolve => setTimeout(resolve, waitTime));
		}
	}
}

/**
 *
 * @param {(() => mssql.ISqlType) | mssql.ISqlType} data_type
 * @param {undefined} defaultType
 * @returns {EvidenceType | undefined}
 */
function nativeTypeToEvidenceType(data_type, defaultType = undefined) {
	switch (data_type) {
		case mssql.TYPES.Int:
		case mssql.TYPES.TinyInt:
		case mssql.TYPES.BigInt:
		case mssql.TYPES.SmallInt:
		case mssql.TYPES.Float:
		case mssql.TYPES.Real:
		case mssql.TYPES.Decimal:
		case mssql.TYPES.Numeric:
		case mssql.TYPES.SmallMoney:
		case mssql.TYPES.Money:
			return EvidenceType.NUMBER;

		case mssql.TYPES.DateTime:
		case mssql.TYPES.SmallDateTime:
		case mssql.TYPES.DateTimeOffset:
		case mssql.TYPES.Date:
		case mssql.TYPES.DateTime2:
			return EvidenceType.DATE;

		case mssql.TYPES.VarChar:
		case mssql.TYPES.NVarChar:
		case mssql.TYPES.Char:
		case mssql.TYPES.NChar:
		case mssql.TYPES.Xml:
		case mssql.TYPES.Text:
		case mssql.TYPES.NText:
			return EvidenceType.STRING;

		case mssql.TYPES.Bit:
			return EvidenceType.BOOLEAN;

		case mssql.TYPES.Time:
		case mssql.TYPES.UniqueIdentifier:
		case mssql.TYPES.Binary:
		case mssql.TYPES.VarBinary:
		case mssql.TYPES.Image:
		case mssql.TYPES.TVP:
		case mssql.TYPES.UDT:
		case mssql.TYPES.Geography:
		case mssql.TYPES.Geometry:
		case mssql.TYPES.Variant:
		default:
			return defaultType;
	}
}

/**
 *
 * @param {mssql.IColumnMetadata} fields
 * @returns
 */
const mapResultsToEvidenceColumnTypes = function (fields) {
	return Object.values(fields).map((field) => {
		/** @type {TypeFidelity} */
		let typeFidelity = TypeFidelity.PRECISE;
		let evidenceType = nativeTypeToEvidenceType(field.type);
		if (!evidenceType) {
			typeFidelity = TypeFidelity.INFERRED;
			evidenceType = EvidenceType.STRING;
		}
		return {
			name: field.name,
			evidenceType: evidenceType,
			typeFidelity: typeFidelity
		};
	});
};

const buildConfig = function (database) {
	if (!database || typeof database !== 'object') {
		throw new Error('Database configuration is required and must be an object');
	}

	const trust_server_certificate = database.trust_server_certificate ?? 'false';
	const encrypt = database.encrypt ?? 'true';
	const connection_timeout = database.connection_timeout ?? 30000;
	const request_timeout = database.request_timeout ?? 30000;
	const database_port = database.connection_port ?? 1433;

	const credentials = {
		user: database.user,
		server: database.server,
		database: database.database,
		password: database.password,
		port: parseInt(database_port),
		connectionTimeout: parseInt(connection_timeout),
		requestTimeout: parseInt(request_timeout),
		options: {
			trustServerCertificate: trust_server_certificate === 'true' || trust_server_certificate === true,
			encrypt: encrypt === 'true' || encrypt === true
		}
	};

	if (database.authenticationType === 'default') {
		return credentials;
	} else if (database.authenticationType === 'azure-active-directory-default') {
		credentials.authentication = {
			type: 'azure-active-directory-default'
		};
		return credentials;
	} else if (database.authenticationType === 'azure-active-directory-access-token') {
		credentials.authentication = {
			type: 'azure-active-directory-access-token',
			options: {
				token: database.attoken
			}
		};
		return credentials;
	} else if (database.authenticationType === 'azure-active-directory-password') {
		credentials.authentication = {
			type: 'azure-active-directory-password',
			options: {
				userName: database.pwuname,
				password: database.pwpword,
				clientId: database.pwclientid,
				tenantId: database.pwtenantid
			}
		};
		return credentials;
	} else if (database.authenticationType === 'azure-active-directory-service-principal-secret') {
		credentials.authentication = {
			type: 'azure-active-directory-service-principal-secret',
			options: {
				clientId: database.spclientid,
				clientSecret: database.spclientsecret,
				tenantId: database.sptenantid
			}
		};
		return credentials;
	}
};

let poolPromise = null;

const getPool = async (config) => {
	if (!poolPromise) {
		poolPromise = mssql.connect(config).catch(err => {
			poolPromise = null; // Reset on failure
			throw err;
		});
	}
	return poolPromise;
};

/** @type {import("@evidence-dev/db-commons").RunQuery<MsSQLOptions>} */
const runQuery = async (queryString, database = {}, batchSize = 100000) => {
	// Define retryable MSSQL error messages or codes
	const retryableErrors = [
		'ETIMEOUT',	// Connection timeout
		'ECONNREFUSED', // Connection refused
		'RequestError: Timeout', // Query timeout
		'A network-related or instance-specific error occurred', // General network error
		'Connection lost', // Connection dropped
		'Failed to connect' // Connection timing or network error
	];

	const queryExecution = async () => {
		const config = buildConfig(database);
		const pool = await getPool(config); // Reuse pool

		const cleaned_string = cleanQuery(queryString);
		const expected_count = await pool
			.request()
			.query(`SELECT COUNT(*) as expected_row_count FROM (${cleaned_string}) as subquery`)
			.catch(() => null);
		const expected_row_count = expected_count?.recordset[0].expected_row_count;

		const request = new mssql.Request(pool);
		request.stream = true;
		request.query(queryString);

		const columns = await new Promise((res) => request.once('recordset', res));
		const stream = request.toReadableStream();
		const results = await asyncIterableToBatchedAsyncGenerator(stream, batchSize);
		results.columnTypes = mapResultsToEvidenceColumnTypes(columns);
		results.expectedRowCount = expected_row_count;

		return results;
	};

	try {
		return await retry(queryExecution, {
			retries: 3,			// Try 3 times (total of 4 attempts)
			delay: 1000,		// Start with 1 second delay
			backoffFactor: 2,	// Exponential backoff: 1s, 2s, 4s
			retryableErrors		// Only retry on these errors
		});
	} catch (err) {
		// Normalise error before replace
		const errStr = (typeof err === 'string')
			? err
			: (err?.message ?? err?.toString?.() ?? 'Unknown error');
		
		throw errStr.replace(/\n|\r/g, ' ');
	}
};

module.exports = runQuery;

/**
 * @typedef {Object} MsSQLOptions
 * @property {string} user
 * @property {string} host
 * @property {string} database
 * @property {string} password
 * @property {`${number}`} connection_port
 * @property {`${boolean}`} trust_server_certificate
 * @property {`${boolean}`} encrypt
 * @property {`${number}`} connection_timeout
 * @property {`${number}`} request_timeout
 * @property {`${number}`} batch_size
 */

/** @type {import('@evidence-dev/db-commons').GetRunner<MsSQLOptions>} */
module.exports.getRunner = async (opts) => {
	const batchSize = opts.batch_size || 10000;
	return async (queryContent, queryPath) => {
		// Filter out non-sql files
		if (!queryPath.endsWith('.sql')) return null;
		return runQuery(queryContent, opts, batchSize);
	};
};

/** @type {import('@evidence-dev/db-commons').ConnectionTester<MsSQLOptions>} */
module.exports.testConnection = async (opts) => {
	return await runQuery('SELECT 1 AS TEST;', opts) //
		.then(exhaustStream)
		.then(() => true)
		.catch((e) => ({ reason: e.message ?? (e.toString() || 'Invalid Credentials') }));
};

module.exports.options = {
	authenticationType: {
		title: 'Authentication type',
		type: 'select',
		secret: false,
		nest: false,
		required: true,
		default: 'sqlauth',
		options: [
			{
				value: 'default',
				label: 'SQL Server Password Authentication'
			},
			{
				value: 'azure-active-directory-default',
				label: 'Entra AD Automatic'
			},
			{
				value: 'azure-active-directory-access-token',
				label: 'Entra AD Access Token'
			},
			{
				value: 'azure-active-directory-password',
				label: 'Entra AD Password Authentication'
			},
			{
				value: 'azure-active-directory-service-principal-secret',
				label: 'Entra AD Service Principal Secret'
			}
		],
		children: {
			default: {
				user: {
					title: 'Username',
					secret: false,
					type: 'string',
					required: true
				},
				password: {
					title: 'Password',
					secret: true,
					type: 'string',
					required: true
				}
			},
			'azure-active-directory-default': {},
			'azure-active-directory-access-token': {
				attoken: {
					title: 'Access Token',
					type: 'string',
					secret: true,
					required: true
				}
			},
			'azure-active-directory-password': {
				pwuname: {
					title: 'User',
					type: 'string',
					secret: false,
					required: true
				},
				pwpword: {
					title: 'Pstring',
					type: 'string',
					secret: true,
					required: true
				},
				pwclientid: {
					title: 'Client ID',
					type: 'string',
					secret: true,
					required: true
				},
				pwtenantid: {
					title: 'Tenant ID',
					type: 'string',
					secret: true,
					required: true
				}
			},
			'azure-active-directory-service-principal-secret': {
				spclientid: {
					title: 'Client ID',
					type: 'string',
					secret: true,
					required: true
				},
				spclientsecret: {
					title: 'Client Secret',
					type: 'string',
					secret: true,
					required: true
				},
				sptenantid: {
					title: 'Tenant ID',
					type: 'string',
					secret: true,
					required: true
				}
			}

			// TODO: authentication types that are not supported yet:
			// - tediousjs.github.io/tedious/api-connection.html
			// - [ ] ntlm
			// - [ ] azure-active-directory-msi-vm
			// - [ ] azure-active-directory-msi-app-service
			// - [x] default
			// - [x] azure-active-directory-default
			// - [x] azure-active-directory-password
			// - [x] azure-active-directory-access-token
			// - [x] azure-active-directory-service-principal-secret
		}
	},
	server: {
		title: 'Host',
		secret: false,
		type: 'string',
		required: true
	},
	database: {
		title: 'Database',
		secret: false,
		type: 'string',
		required: true
	},
	connection_port: {
		title: 'Port',
		secret: false,
		type: 'number',
		required: false,
		default: 1433
	},
	trust_server_certificate: {
		title: 'Trust Server Certificate',
		secret: false,
		type: 'boolean',
		description: 'Toggle server certificate trust (default: true)',
		default: true
	},
	encrypt: {
		title: 'Encrypt',
		secret: false,
		type: 'boolean',
		default: true,
		description: 'Toggle database connection encryption (default: true)'
	},
	connection_timeout: {
		title: 'Connection Timeout',
		secret: false,
		type: 'number',
		required: false,
		description: 'Connection timeout in milliseconds (ms)'
	},
	request_timeout: {
		title: 'Request Timeout',
		secret: false,
		type: 'number',
		required: false,
		description: 'Request timeout in milliseconds (ms)'
	},
	batch_size: {
		title: 'Batch Size',
		secret: false,
		type: 'number',
		required: false,
		default: 10000,
		description: 'Number of rows to process per database transaction'
	}
};

process.on('exit', () => {
	if (poolPromise) poolPromise.then(pool => pool.close());
});