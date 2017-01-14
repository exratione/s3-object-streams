/**
 * @fileOverview A simple example to illustrate calculation of bucket usage.
 */

// Core.
var fs = require('fs');
var zlib = require('zlib');

// NPM.
var csv = require('csv');
var _ = require('lodash');
var s3ObjectStreams = require('s3-object-streams');

// Assuming that we already have the manifest JSON and a gzipped CSV data file
// downloaded from S3:
var manifest = require('/path/to/manifest.json');
var readStream = fs.createReadStream('/path/to/data.csv.gz');

var s3InventoryUsageStream = new s3ObjectStreams.S3InventoryUsageStream({
  // Determine folders from keys with this delimiter.
  delimiter: '/',
  // Group one level deep into the folders.
  depth: 1,
  // Only send a running total once every 100 objects.
  outputFactor: 100
});

var runningTotals;

// Log all of the listed objects.
s3UsageStream.on('data', function (totals) {
  runningTotals = totals;
  console.info(JSON.stringify(runningTotals, null, '  '));
});

var complete = _.once(function (error) {
  if (error) {
    console.error(error);
  }
  else {
    console.info('Complete');
  }
});

var gunzip = zlib.createGunzip();
var csvParser = csv.parse({
  // The manifest file defines the columns for the CSV. Specifying them here
  // ensures that the parser returns objects with properties named for the
  // columns.
  columns: manifest.fileSchema.split(', ')
});

csvParser.on('error', complete);
gunzip.on('error', complete);
readStream.on('error', complete);
s3InventoryUsageStream.on('error', complete);
s3InventoryUsageStream.on('end', complete);

// Unzip the file on the fly, feed it to the csvParser, and then into the
// object stream.
readStream.pipe(gunzip).pipe(csvParser).pipe(transformer).pipe(s3InventoryUsageStream);
