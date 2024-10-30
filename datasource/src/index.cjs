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

function getAuthenticationMethod (database, authtype) {
	var authentication_method = authtype;

	if (authentication_method === 'aadpassword') {
		return {
			authentication: {
				type: 'azure-active-directory-password',
				options: {
					"userName": database.pwuname,
					"password": database.pwpword,
					"clientId": database.pwclientid,
					"tenantId": database.pwtenantid
				}
			},
			server: database.server,
			database: database.database,
			options: {
				trustServerCertificate: true,
				port: parseInt(database.port ?? 1433),
				encrypt: true
			}
		};
	}
	else if (authentication_method === 'aadaccesstoken'){
		return {
			authentication: {
				type: 'azure-active-directory-access-token',
				options: {
					"token": database.attoken
				}
			},
			server: database.server,
			database: database.database,
			options: {
				trustServerCertificate: true,
				port: parseInt(database.port ?? 1433),
				encrypt: true
			}
		};
	}
	else if (authentication_method === 'aadserviceprincipal'){
		return {
			authentication: {
				type: 'azure-active-directory-service-principal-secret',
				options: {
					"clientId": database.spclientid,
					"clientSecret": database.spclientsecret,
					"tenantId": database.sptenantid
				}
			},
			server: database.server,
			database: database.database,
			options: {
				trustServerCertificate: true,
				port: parseInt(database.port ?? 1433),
				encrypt: true
			}
		};
	}
	else if (authentication_method === 'aaddefault') {
		return {
			authentication: {
				type: 'azure-active-directory-default'
			},
			server: database.server,
			database: database.database,
			options: {
				trustServerCertificate: true,
				port: parseInt(database.port ?? 1433),
				encrypt: true
			}
		};
	}
};

/** @type {import("@evidence-dev/db-commons").RunQuery<MsSQLOptions>} */
const runQuery = async (queryString, database = {}, batchSize = 100000) => {
	try {
		/* Parse authentication type and construct credentials string */
		const authtype = database.authenticator ?? 'aadaccesstoken';
		const credentials = getAuthenticationMethod(database, authtype)

		/* Connect to the SQL endpoint */
		const pool = await mssql.connect(credentials);
		const cleaned_string = cleanQuery(queryString);
		const expected_count = await pool
			.request()
			.query(`SELECT COUNT(*) as expected_row_count FROM (${cleaned_string}) as subquery`)
			.catch(() => null);
		const expected_row_count = expected_count?.recordset[0].expected_row_count;

		const request = new mssql.Request();
		request.stream = true;
		request.query(queryString);

		const columns = await new Promise((res) => request.once('recordset', res));

		const stream = request.toReadableStream();
		const results = await asyncIterableToBatchedAsyncGenerator(stream, batchSize, {
			closeConnection: () => pool.close()
		});
		results.columnTypes = mapResultsToEvidenceColumnTypes(columns);
		results.expectedRowCount = expected_row_count;

		return results;
	} catch (err) {
		if (err.message) {
			console.log(err.message)
			throw err.message.replace(/\n|\r/g, ' ');
		} else {
			console.log(err)
			/*throw err.replace(/\n|\r/g, ' ');*/
		}
	}
};

module.exports = runQuery;

/**
 * @typedef {Object} MsSQLOptions
 * @property {string} host
 * @property {string} database
 * @property {string} pwuname
 * @property {string} pwpname
 * @property {string} pwclientid
 * @property {string} pwtenantid
 * @property {string} attoken
 * @property {string} spclientid
 * @property {string} spclientsecret
 * @property {string} sptenantid
 * @property {`${number}`} port
 * @property {`${boolean}`} trust_server_certificate
 * @property {`${boolean}`} encrypt
 */

/** @type {import('@evidence-dev/db-commons').GetRunner<MsSQLOptions>} */
module.exports.getRunner = async (opts) => {
	return async (queryContent, queryPath, batchSize) => {
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
	authenticator: {
		title: 'Authentication Method',
		type: 'select',
		secret: false,
		nest: false,
		required: true,
		default: 'aadaccesstoken',
		options: [
			{
				value: 'aadaccesstoken',
				label: 'Access Token'
			},
			{
				value: 'aadpassword',
				label: 'Password'
			},
			{
				value: 'aadserviceprincipal',
				label: 'Service Principal Secret'
			},
			{
				value: 'aaddefault',
				label: 'Automatic'
			}
		],
		children: {
			'aadpassword': {
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
			'aadaccesstoken': {
				attoken: {
					title: 'Access Token',
					type: 'string',
					secret: true,
					required: true
				}
			},
			'aadserviceprincipal': {
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
	port: {
		title: 'Port',
		secret: false,
		type: 'number',
		required: false
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
	}
};
