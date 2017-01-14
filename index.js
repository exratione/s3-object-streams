/**
 * @fileOverview Main interface to the package.
 */

// Local.
var S3ListObjectStream = require('./lib/stream/s3ListObjectStream');
var S3ConcurrentListObjectStream = require('./lib/stream/s3ConcurrentListObjectStream');
var S3UsageStream = require('./lib/stream/s3UsageStream');
var S3InventoryUsageStream = require('./lib/stream/s3InventoryUsageStream');

// Expose the constructors.
exports.S3ListObjectStream = S3ListObjectStream;
exports.S3ConcurrentListObjectStream = S3ConcurrentListObjectStream;
exports.S3UsageStream = S3UsageStream;
exports.S3InventoryUsageStream = S3InventoryUsageStream;
