# S3 Object Streams

A small Node.js package that can be helpful when performing operations on very
large S3 buckets, those containing millions of objects or more, or when
processing S3 Inventory listings for those buckets. Streaming the listed
contents keeps memory under control and using the Streams API allows for fairly
compact utility code.

For very large buckets, S3 Inventory is always a better choice.

  * [S3ListObjectStream](#s3listobjectstream)
  * [S3ConcurrentListObjectStream](#s3concurrentlistobjectstream)
  * [S3UsageStream](#s3usagestream)
  * [S3InventoryUsageStream](#s3inventoryusagestream)

## Installing

Obtain via NPM:

```
npm install s3-object-streams
```

## S3ListObjectStream

An object stream that pipes in configuration objects for listing the contents of
an S3 bucket, and pipes out S3 object definitions.

```js
var AWS = require('aws-sdk');
var s3ObjectStreams = require('s3-object-streams');

var s3ListObjectStream = new s3ObjectStreams.S3ListObjectStream();
var s3Client = new AWS.S3();

// Log all of the listed objects.
s3ListObjectStream.on('data', function (s3Object) {
  console.info(s3Object);
});
s3ListObjectStream.on('end', function () {
  console.info('Listing complete.')
});
s3ListObjectStream.on('error', function (error) {
  console.error(error);
});

// List the contents of a couple of different buckets.
s3ListObjectStream.write({
  s3Client: s3Client,
  bucket: 'exampleBucket1',
  // Optional, only list keys with the given prefix.
  prefix: 'examplePrefix/',
  // Optional, group keys using a delimiter.
  // delimiter: '/',
  // Optional, defaults to 1000. The number of objects per request.
  maxKeys: 1000
});
s3ListObjectStream.write({
  s3Client: s3Client,
  bucket: 'exampleBucket2'
});
s3ListObjectStream.end();
```

Objects emitted by the stream have the standard format, with the addition of a
`Bucket` property:

```js
{
  Bucket: 'exampleBucket1',
  Key: 'examplePrefix/file.txt',
  LastModified: Date.now(),
  ETag: 'tag string',
  Size: 200,
  StorageClass: 'STANDARD',
  Owner: {
    DisplayName: 'exampleowner',
    ID: 'owner ID'
  }
```

## S3ConcurrentListObjectStream

This works in the same way as the `S3ListObjectStream`, but under the hood it
splits up the bucket by common prefixes and then recursively lists objects under
each common prefix concurrently, up to the maximum specified concurrency.

```js
var AWS = require('aws-sdk');
var s3ObjectStreams = require('s3-object-streams');

var s3ConcurrentListObjectStream = new s3ObjectStreams.S3ConcurrentListObjectStream({
  // Optional, defaults to 15.
  maxConcurrency: 15
});
var s3Client = new AWS.S3();

// Log all of the listed objects.
s3ConcurrentListObjectStream.on('data', function (s3Object) {
  console.info(s3Object);
});
s3ConcurrentListObjectStream.on('end', function () {
  console.info('Listing complete.')
});
s3ConcurrentListObjectStream.on('error', function (error) {
  console.error(error);
});

// List the contents of a couple of different buckets.
s3ConcurrentListObjectStream.write({
  s3Client: s3Client,
  bucket: 'exampleBucket1',
  // Optional, only list keys with the given prefix.
  prefix: 'examplePrefix/',
  // Optional, group keys using a delimiter.
  // delimiter: '/',
  // Optional, defaults to 1000. The number of objects per request.
  maxKeys: 1000
});
s3ConcurrentListObjectStream.write({
  s3Client: s3Client,
  bucket: 'exampleBucket2'
});
s3ConcurrentListObjectStream.end();
```

Objects emitted by the stream have the standard format, with the addition of a
`Bucket` property:

```js
{
  Bucket: 'exampleBucket1',
  Key: 'examplePrefix/file.txt',
  LastModified: Date.now(),
  ETag: 'tag string',
  Size: 200,
  StorageClass: 'STANDARD',
  Owner: {
    DisplayName: 'exampleowner',
    ID: 'owner ID'
  }
```

## S3UsageStream

A stream for keeping a running total of count and size of listed S3 objects by
bucket and key prefix. Useful for applications with a UI that needs to track
progress.

```js
var AWS = require('aws-sdk');
var s3ObjectStreams = require('s3-object-streams');

var s3ListObjectStream = new s3ObjectStreams.S3ListObjectStream();
var s3UsageStream = new s3ObjectStreams.S3UsageStream({
  // Determine folders from keys with this delimiter.
  delimiter: '/',
  // Group one level deep into the folders.
  depth: 1,
  // Only send a running total once every 100 objects.
  outputFactor: 100
});
var s3Client = new AWS.S3();

s3ListObjectStream.pipe(s3UsageStream);

var runningTotals;

// Log all of the listed objects.
s3UsageStream.on('data', function (totals) {
  runningTotals = totals;
  console.info(runningTotals);
});
s3UsageStream.on('end', function () {
  console.info('Final total: ', runningTotals);
});
s3UsageStream.on('error', function (error) {
  console.error(error);
});

// Obtain the total usage for these two buckets.
s3ListObjectStream.write({
  s3Client: s3Client,
  bucket: 'exampleBucket1'
});
s3ListObjectStream.write({
  s3Client: s3Client,
  bucket: 'exampleBucket2'
});
s3ListObjectStream.end();
```

The running total objects emitted by the stream have the following format:

```js
{
  path: 'exampleBucket/folder1',
  storageClass: {
    STANDARD: {
      // The number of files of this storage class.
      count: 55,
      // Total size in bytes of files in this storage class.
      size: 1232983
    },
    STANDARD_IA: {
      count: 0,
      size: 0
    },
    REDUCED_REDUNDANCY: {
      count: 2,
      size: 5638
    },
    GLACIER: {
      count: 0,
      size: 0
    }
  }
}
```

## S3InventoryUsageStream

A stream for keeping a running total of count and size of S3 objects by bucket
and key prefix, accepting objects from an S3 Inventory CSV file rather than
from the `listObjects` API endpoint.

```js
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
```

The running total objects emitted by the stream have the following format:

```js
{
  path: 'exampleBucket/folder1',
  storageClass: {
    STANDARD: {
      // The number of files of this storage class.
      count: 55,
      // Total size in bytes of files in this storage class.
      size: 1232983
    },
    STANDARD_IA: {
      count: 0,
      size: 0
    },
    REDUCED_REDUNDANCY: {
      count: 2,
      size: 5638
    },
    GLACIER: {
      count: 0,
      size: 0
    }
  }
}
```
