# S3 Object Streams

A small Node.js package that can be helpful when performing operations on very
large S3 buckets, those containing millions of objects or more. Streaming the
listed contents keeps memory under control and using the Streams API allows for
fairly compact utility code.

## S3ListObjectStream

An object stream that pipes in configuration objects for listing the contents of
an S3 bucket, and pipes out S3 object definitions.

```
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
```
