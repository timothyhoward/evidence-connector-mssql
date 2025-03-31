const {
	EvidenceType,
	TypeFidelity,
	asyncIterableToBatchedAsyncGenerator,
	cleanQuery,
	exhaustStream
} = require('@evidence-dev/db-commons');
const mssql = require('mssql');

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

/**
 * Sleep for a specified amount of time
 * @param {number} ms - milliseconds to sleep
 * @returns {Promise<void>}
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Executes a function with retry logic
 * @param {Function} fn - Function to execute
 * @param {Object} options - Retry options
 * @param {number} options.maxRetries - Maximum number of retry attempts
 * @param {number} options.baseDelay - Base delay between retries in ms
 * @param {number} options.maxDelay - Maximum delay between retries in ms
 * @returns {Promise<any>} - The result of the function execution
 */
const withRetry = async (fn, options = {}) => {
	const {
		maxRetries = 3,
		baseDelay = 1000,
		maxDelay = 10000
	} = options;
	
	// Default retry conditions - connection, timeout, and transient errors
	const retryableErrors = [
		'ETIMEOUT',
		'ECONNRESET',
		'ECONNREFUSED',
		'ESOCKET',
		'PROTOCOL_SEQUENCE_TIMEOUT',
		'EALREADYCONNECTED',
		'EALREADYCONNECTING',
		'Failed to connect',
		'Connection lost',
		'Timeout',
		'Socket hang up',
		'Network error',
		'Request failed',
		'Deadlock',
		'The connection has been lost',
		'The server has reset the connection'
	];
	
	const shouldRetry = (err) => {
		// Check if error message contains any of the retryable error messages
		if (err && err.message) {
			return retryableErrors.some(errMsg => 
				err.message.toLowerCase().includes(errMsg.toLowerCase())
			);
		}
		
		// If error is a string, check if it contains any of the retryable error messages
		if (typeof err === 'string') {
			return retryableErrors.some(errMsg => 
				err.toLowerCase().includes(errMsg.toLowerCase())
			);
		}
		
		return false;
	};
	
	let attempts = 0;
	
	while (true) {
		try {
			return await fn();
		} catch (err) {
			attempts++;
			
			// If we've exceeded max retries or error isn't retryable, throw the error
			if (attempts >= maxRetries || !shouldRetry(err)) {
				throw err;
			}
			
			// Calculate exponential backoff with jitter
			const delay = Math.min(
				maxDelay,
				baseDelay * Math.pow(2, attempts - 1) * (1 + Math.random() * 0.2)
			);
			
			console.warn(`Database operation failed (attempt ${attempts}/${maxRetries}). Retrying in ${Math.round(delay)}ms. Error: ${err.message || err}`);
			
			// Wait before retrying
			await sleep(delay);
		}
	}
};

/** @type {import("@evidence-dev/db-commons").RunQuery<MsSQLOptions>} */
const runQuery = async (queryString, database = {}, batchSize = 100000) => {
	let pool;

	try {
		const config = buildConfig(database);
		const retryOptions = database.retryOptions || {};

		// Connect to the database with retry
		pool = await withRetry(
			async () => mssql.connect(config),
			retryOptions
		);

		const cleaned_string = cleanQuery(queryString);

		// Execute COUNT query with retry
		const expected_count = await withRetry(
			async () => pool.request().query(`SELECT COUNT(*) as expected_row_count FROM (${cleaned_string}) as subquery`),
			retryOptions
		).catch(() => null);
		
		const expected_row_count = expected_count?.recordset[0].expected_row_count;

		// Execute main query with retry
		return await withRetry(async () => {
			const request = new mssql.Request(pool);
			request.stream = true;
			
			// Create a promise that resolves with the columns when recordset event is emitted
			const columnsPromise = new Promise((resolve) => {
				request.once('recordset', resolve);
			});
			
			// Execute the query
			request.query(queryString);
			
			// Wait for columns to be available
			const columns = await columnsPromise;
			
			const stream = request.toReadableStream();
			const results = await asyncIterableToBatchedAsyncGenerator(stream, batchSize, {
				closeConnection: () => pool?.close()
			});
			
			results.columnTypes = mapResultsToEvidenceColumnTypes(columns);
			results.expectedRowCount = expected_row_count;
			
			return results;
		}, retryOptions);
	
	} catch (err) {
		// Ensure pool is closed on error
		if (pool) {
			try {
				await pool.close();
			} catch (closeErr) {
				console.error('Error closing pool after failure:', closeErr);
			}
		}

		if (err.message) {
			throw err.message.replace(/\n|\r/g, ' ');
		} else {
			throw err.replace(/\n|\r/g, ' ');
		}
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
 * @property {Object} [retryOptions] - Options for retry functionality
 * @property {number} [retryOptions.maxRetries] - Maximum number of retry attempts
 * @property {number} [retryOptions.baseDelay] - Base delay between retries in ms
 * @property {number} [retryOptions.maxDelay] - Maximum delay between retries in ms
 */

/** @type {import('@evidence-dev/db-commons').GetRunner<MsSQLOptions>} */
module.exports.getRunner = async (opts) => {
	return async (queryContent, queryPath, batchSize) => {
		// Filter out non-sql files
		if (!queryPath.endsWith('.sql')) return null;
		return runQuery(queryContent, opts, batchSize);
	};
};

/** @type {import('@evidence-dev/db-commons').ProcessSource<MsSQLOptions>} */
module.exports.processSource = async function (sourceConfig, queryContent, queryPath) {
	// Return null for non-SQL files
	if (!queryPath.endsWith('.sql')) return null;

	// Process the SQL query through the database
	return runQuery(queryContent, sourceConfig, sourceConfig.batchSize || 100000);
};

/** @type {import('@evidence-dev/db-commons').ConnectionTester<MsSQLOptions>} */
module.exports.testConnection = async (opts) => {
	const retryOptions = opts.retryOptions || {};
	
	return await withRetry(
		async () => runQuery('SELECT 1 AS TEST;', opts)
			.then(exhaustStream)
			.then(() => true)
			.catch((e) => ({ reason: e.message ?? (e.toString() || 'Invalid Credentials') })),
		retryOptions
	);
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
				label: 'SQL Login'
			},
			{
				value: 'azure-active-directory-default',
				label: 'DefaultAzureCredential'
			},
			{
				value: 'azure-active-directory-access-token',
				label: 'Access token'
			},
			{
				value: 'azure-active-directory-password',
				label: 'Entra ID User/Password'
			},
			{
				value: 'azure-active-directory-service-principal-secret',
				label: 'Service Principal Secret'
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
		description: 'Should be true for local dev / self-signed certificates',
		default: false
	},
	encrypt: {
		title: 'Encrypt',
		secret: false,
		type: 'boolean',
		default: false,
		description: 'Should be true when using azure'
	},
	connection_timeout: {
		title: 'Connection Timeout',
		secret: false,
		type: 'number',
		required: false,
		description: 'Connection timeout in ms'
	},
	request_timeout: {
		title: 'Request Timeout',
		secret: false,
		type: 'number',
		required: false,
		description: 'Request timeout in ms'
	},
	retryOptions: {
		title: 'Retry Options',
		type: 'object',
		required: false,
		description: 'Options for database retry functionality',
		properties: {
			maxRetries: {
				title: 'Maximum Retries',
				type: 'number',
				required: false,
				default: 3,
				description: 'Maximum number of retry attempts'
			},
			baseDelay: {
				title: 'Base Delay',
				type: 'number',
				required: false,
				default: 1000,
				description: 'Base delay between retries in ms'
			},
			maxDelay: {
				title: 'Maximum Delay',
				type: 'number',
				required: false,
				default: 10000,
				description: 'Maximum delay between retries in ms'
			}
		}
	}
};