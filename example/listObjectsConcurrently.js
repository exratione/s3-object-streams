/**
 * @fileOverview Listing objects concurrently. Much faster.
 */

// NPM.
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
  prefix: 'examplePrefix',
  // Optional, defaults to 1000. The number of objects per request.
  maxKeys: 1000
});
s3ConcurrentListObjectStream.write({
  s3Client: s3Client,
  bucket: 'exampleBucket2'
});
s3ConcurrentListObjectStream.end();
