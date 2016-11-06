/**
 * @fileOverview S3UsageStream class definition.
 */

// Core.
var Transform = require('stream').Transform;
var util = require('util');

// NPM.
var _ = require('lodash');

//---------------------------------------------------------------------------
// Class constructor.
//---------------------------------------------------------------------------

/**
 * @class An object stream for running totals of size and count of S3 objects.
 *
 * Pipe in S3 object definitions from the S3ListObjectStream, pipe out running
 * total objects with counts and size grouped by bucket and key prefix:
 *
 * [
 *   {
 *     path: 'bucket/folder1',
 *     // Number of objects counted so far.
 *     count: 55,
 *     // Total size of all objects counted so far in bytes.
 *     size: 1232983
 *   }
 *   ,
 * ]
 *
 * @param {Object} options Standard stream options, plus those noted.
 * @param {Object} [options.delimiter] How to split keys into folders. Defaults
 *   to '/',
 * @param {Object} [options.depth] Depth of folders to group count and size.
 *   Defaults to 0, or no folders, just buckets.
 * @param {Number} [options.outputFactor] At 1, send an update for every object.
 *   At 10, only once every 10 objects. Defaults to 100. Updating is expensive
 *   for very large buckets or depths.
 */
function S3UsageStream (options) {
  options = options || {};
  // Important; make this an object stream.
  options.objectMode = true;

  this.delimiter = options.delimiter || '/';
  this.depth = options.depth || 0;
  this.outputFactor = options.outputFactor || 100;

  // The running totals.
  this.totals = {};
  this.sortedTotals = [];
  this.count = 0;

  S3UsageStream.super_.call(this, options);
}

util.inherits(S3UsageStream, Transform);

//---------------------------------------------------------------------------
// Methods
//---------------------------------------------------------------------------

/**
 * Update the running totals and return a copy.
 *
 * @param {Object} s3Object An S3 object definition.
 */
S3UsageStream.prototype.updateTotals = function (s3Object) {
  var self = this;
  var paths = [s3Object.Bucket];
  var pathSegments;
  var index;
  var limit;

  // Assemble paths. For something like s3://bucket/a/b/c, depending on depth we
  // want 'bucket', 'bucket/a', 'bucket/a/b', and so on.
  if (this.depth > 0) {
    pathSegments = s3Object.Key.split(this.delimiter);
    // Drop the last segment, which is the file name.
    pathSegments.pop();
    limit = Math.min(this.depth, pathSegments.length);

    for (index = 0; index < limit; index++) {
      paths.push(
        [s3Object.Bucket].concat(pathSegments.slice(0, index + 1)).join(this.delimiter)
      );
    }
  }

  // Run through each of the paths, add the totals.
  paths.forEach(function (path) {
    if (!self.totals[path]) {
      self.totals[path] = {
        path: path,
        count: 0,
        size: 0
      };

      var index = _.sortedIndexBy(self.sortedTotals, self.totals[path], function (item) {
        return item.path;
      });

      self.sortedTotals.splice(index, 0, self.totals[path]);
    }

    self.totals[path].count++;
    self.totals[path].size += s3Object.Size;
  });

  // Sending on a cloned object is an expensive operation for very large buckets
  // or large depths, meaning a large number of items in the array. Increment
  // the count, and only send on an object every outputFactor times.
  this.count++;

  if (this.count % this.outputFactor === 0) {
    this.push(_.cloneDeep(this.sortedTotals));
  }
};

/**
 * Override end() to ensure that at least one output is delivered despite the
 * outputFactor setting. This will mean that there will be a duplicate sent for
 * some circumstances, but that isn't important in this usage.
 */
S3UsageStream.prototype.end = function () {
  this.push(_.cloneDeep(this.sortedTotals));
  S3UsageStream.super_.prototype.end.apply(this, arguments);
};

/**
 * Implementation of the necessary transform method.
 *
 * @param {Object} data An S3 object definition.
 * @param {String} encoding Irrelevant since this is an object stream.
 * @param {Function} callback Invoked after this listing is processed.
 */
S3UsageStream.prototype._transform = function (data, encoding, callback) {
  if (
    !data ||
    typeof data.Bucket !== 'string' ||
    typeof data.Key !== 'string' ||
    typeof data.Size !== 'number'
  ) {
    return callback(new Error('Invalid S3 object definition provided: ' + JSON.stringify(data)));
  }

  this.updateTotals(data);
  callback();
};

//---------------------------------------------------------------------------
// Export class constructor.
//---------------------------------------------------------------------------

module.exports = S3UsageStream;

