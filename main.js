require('dotenv').config();
const yargs = require('yargs');
const fs = require('fs');

const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');

const bigquery = new BigQuery();
const storage = new Storage();

const args = yargs(process.argv.slice(2))
  .options({
    'datasetId': {
      alias: ['datasetid', 'd'],
      demandOption: true,
      describe: 'The name of the existing dataset into which to load the data.',
      type: 'string'
    },
    'tableId': {
      alias: ['tableid', 't'],
      demandOption: true,
      describe: 'The name of the table into which to load the data. It does not have to exist.',
      type: 'string'
    },
    'bucketName': {
      alias: ['bucketname', 'b'],
      demandOption: true,
      describe: 'The Cloud Storage URI of the bucket where the data is stored.',
      type: 'string'
    },
    'fileName': {
      alias: ['filename', 'fn'],
      demandOption: true,
      describe: 'Supports wildcards as per the BigQuery client library docs.',
      type: 'string'
    },
    'schema': {
      alias: ['s'],
      demandOption: false,
      default: false,
      describe: 'Optional. If the destination table does not already exist, you need to supply a table schema in the file schema.json',
      type: 'boolean'
    },
    'partition': {
      alias: ['p'],
      demandOption: false,
      describe: 'Optional. Defines the type of partition.',
      type: 'string',
      choices: ['time', 'range']
    },
    'sourceFormat': {
      alias: ['sourceformat', 'sf'],
      demandOption: false,
      default: 'CSV',
      describe: 'The filetype of your source data.',
      type: 'string',
      choices: ['CSV', 'DATASTORE_BACKUP', 'NEWLINE_DELIMITED_JSON', 'AVRO', 'PARQUET', 'ORC']
    },
    'location': {
      alias: ['l'],
      demandOption: false,
      default: 'europe-west2',
      describe: 'Optional. The location for the job to take place.',
      type: 'string'
    },
    'writeDisposition': {
      alias: ['writedisposition', 'wd'],
      demandOption: false,
      default: 'WRITE_APPEND',
      describe: 'Optional. Specifies the action that occurs if the destination table already exists. ',
      type: 'string',
      choices: ['WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY']
    },
    'skipLeadingRows': {
      alias: ['skipleadingrows', 'skip'],
      demandOption: false,
      describe: 'Optional. An integer indicating the number of header rows in the source data.',
      type: 'number'
    },
  })
  .parse();
const { datasetId, tableId, bucketName, fileName, schema, partition, sourceFormat, location, writeDisposition, skipLeadingRows } = args;

async function main() {
  if (!datasetId || !tableId || !bucketName || !fileName) {
    throw 'One or more arguments are missing from the command: must include --datasetId, --tableId, --bucketName, --fileName.'
  }
  // Configure the load job. For full list of options, see:
  // https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
  const metadata = {
    sourceFormat,
    skipLeadingRows,
    writeDisposition,
    location,
  };
  if (schema) {
    let rawSchema = fs.readFileSync('schema.json');
    let schemaData = JSON.parse(rawSchema);
    metadata.schema = {
      fields: schemaData.schema.fields
    };
    if (partition && partition == 'time') {
      metadata.timePartitioning = schemaData.timePartitioning;
    }
    if (partition && partition == 'range') {
      metadata.rangePartitioning = schemaData.rangePartitioning;
    }
  }

  const dataset = bigquery.dataset(datasetId);
  const table = dataset.table(tableId);

  console.log(`Loading ${fileName} into ${datasetId}:${tableId} from ${bucketName}...`);
  const [job] = await table.load(storage.bucket(bucketName).file(fileName), metadata);
  console.log(`Job ${job.id} completed.`);

  const errors = job.status.errors;
  if (errors && errors.length > 0) {
    throw errors;
  }
}
main();