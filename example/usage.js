/**
 * @fileOverview A simple example to illustrate calculation of bucket usage.
 */

// NPM.
var AWS = require('aws-sdk');
var s3ObjectStreams = require('s3-object-streams');

var s3ListObjectStream = new s3ObjectStreams.S3ListObjectStream();
var s3UsageStream = new s3ObjectStreams.S3UsageStream({
  delimiter: '/',
  depth: 1
});
var s3Client = new AWS.S3();

s3ListObjectStream.pipe(s3UsageStream);

var runningTotals;

// Log all of the listed objects.
s3UsageStream.on('data', function (totals) {
  runningTotals = totals;
  // Uncomment if interested in seeing the results.
  // console.info(runningTotals);
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
