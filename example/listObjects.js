/**
 * @fileOverview A simple example to list objects.
 */

// NPM.
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
  prefix: 'examplePrefix',
  // Optional, defaults to 1000. The number of objects per request.
  maxKeys: 1000
});
s3ListObjectStream.write({
  s3Client: s3Client,
  bucket: 'exampleBucket2'
});
s3ListObjectStream.end();
