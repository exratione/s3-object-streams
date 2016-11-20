/**
 * @fileOverview S3ConcurrentListObjectStream class definition.
 */

// Core.
var Transform = require('stream').Transform;
var util = require('util');

// NPM.
var async = require('async');
var _ = require('lodash');

//---------------------------------------------------------------------------
// Class constructor.
//---------------------------------------------------------------------------

/**
 * @class An object stream to list S3 objects via multiple concurrent requests.
 *
 * This is considerably faster than S3ListObjectStream, but the level of
 * concurrency is limited by practical concerns. Aside from API throttling,
 * most ordinary machines start to exhibit failures with AWS SDKs at around 20
 * parallel requests.
 *
 * It works by descending through 'directories' defined by splitting keys with
 * the delimiter. For each new 'subdirectory', a new task is spawned to list
 * it and its 'subdirectories'. Those tasks run in parallel up to the maximum
 * concurrency.
 *
 * Pipe in objects of the following form:
 *
 * {
 *   s3Client: new AWS.S3(),
 *   bucket: 'exampleBucket',
 *   // Optional. Maximum degree of concurrency if there are many common
 *   // prefixes. Defaults to 10.
 *   maxConcurrency: 10,
 *   // Optional. Used to find common prefixes that can be listed concurrently.
 *   // Defaults to '/'.
 *   delimiter: '/',
 *   // Optional, defaults to 1000. How many objects to return in one API
 *   // request under the hood.
 *   maxKeys: 1000,
 *   // Optional. If present, only list objects with keys matching the prefix.
 *   prefix: 'examplePrefix'
 * }
 *
 * Pipe out standard response objects from the S3 listObjects API, with the
 * addition of the bucket name as well.
 *
 * {
 *   Bucket: 'exampleBucket',
 *   Key: ...
 *   LastModified: ...
 *   ETag: ...
 *   Size: ...
 *   StorageClass: ...
 *   Owner: {
 *     DisplayName: ...
 *     ID: ...
 *   }
 * }
 *
 * @param {Object} options Standard stream options, plus the following.
 * @param {Number} [options.maxConcurrency] Defaults to 15. Number of
 *   concurrent API requests to make.
 */
function S3ConcurrentListObjectStream (options) {
  options = options || {};
  // Important; make this an object stream.
  options.objectMode = true;
  options.maxConcurrency = options.maxConcurrency || 15;

  // A queue for managing concurrency of API requests.
  this.queue = async.queue(
    this.listDirectoryAndRecuse.bind(this),
    options.maxConcurrency
  );

  S3ConcurrentListObjectStream.super_.call(this, options);
}

util.inherits(S3ConcurrentListObjectStream, Transform);

//---------------------------------------------------------------------------
// Methods
//---------------------------------------------------------------------------

/**
 * Helper function. Send a task to the queue.
 *
 * @param {Object} options
 * @param {AWS.S3} options.s3Client An AWS client instance.
 * @param {String} options.bucket The bucket to list.
 * @param {Number} options.delimiter Used to find common prefixes to split out
 *   requests for object listing. Defaults to '/'.
 * @param {Number} [options.maxKeys] Maximum number of keys to return per
 *   request. Defaults to 1000.
 * @param {String} [options.prefix] If present, only list objects with keys that
 *   match the prefix.
 */
S3ConcurrentListObjectStream.prototype.sendToQueue = function (options) {
  var self = this;

  this.queue.push(options, function (error) {
    if (error) {
      self.emit('error', error);
    }
  });
};

/**
 * List the objects in a given 'directory' by common prefix, and all the
 * common prefixes for 'subdirectories'.
 *
 * @param {Object} options
 * @param {AWS.S3} options.s3Client An AWS client instance.
 * @param {String} options.bucket The bucket name.
 * @param {Number} options.delimiter Used to find common prefixes to split out
 *   requests for object listing. Defaults to '/'.
 * @param {String} [options.prefix] If set only return keys beginning with
 *   the prefix value.
 * @param {String} [options.continuationToken] If set the list only a paged set
 *   of keys, with the token showing the start point.
 * @param {Number} [options.maxKeys] Maximum number of keys to return per
 *   request. Defaults to 1000.
 * @param {Function} callback - Callback of the form
    function (error, nextMarker, Object[], String[]).
 */
S3ConcurrentListObjectStream.prototype.listDirectoryPage = function (
  options,
  callback
) {
  var params = {
    Bucket: options.bucket,
    Delimiter: options.delimiter,
    ContinuationToken: options.continuationToken,
    MaxKeys: options.maxKeys,
    Prefix: options.prefix
  };

  // S3 operations have a small but significant error rate.
  async.retry(
    3,
    function (asyncCallback) {
      options.s3Client.listObjectsV2(params, asyncCallback);
    },
    function (error, response) {
      var continuationToken;

      if (error) {
        return callback(error);
      }

      // Check to see if there are yet more objects to be obtained, and if so
      // return the continuationToken for use in the next request.
      if (response.IsTruncated) {
        continuationToken = response.NextContinuationToken;
      }

      callback(
        null,
        continuationToken,
        response.Contents,
        _.map(response.CommonPrefixes, function (prefixObject) {
          return prefixObject.Prefix;
        })
      );
    }
  );
};

/**
 * List the objects in a given 'directory' by common prefix, and spawn new tasks
 * to list all child 'directories'.
 *
 * All the objects found in this 'directory' are piped onward.
 *
 * @param {Object} options
 * @param {AWS.S3} options.s3Client An AWS client instance.
 * @param {String} options.bucket The bucket to list.
 * @param {Number} options.delimiter Used to find common prefixes to split out
 *   requests for object listing. Defaults to '/'.
 * @param {Number} [options.maxKeys] Maximum number of keys to return per
 *   request. Defaults to 1000.
 * @param {String} [options.prefix] If present, only list objects with keys that
 *   match the prefix.
 * @param {Function} callback Of the form function (error).
 */
S3ConcurrentListObjectStream.prototype.listDirectoryAndRecuse = function (
  options,
  callback
) {
  var self = this;

  /**
   * Recursively list common prefixes.
   *
   * @param {String|undefined} marker A value provided by the S3 API to enable
   *   paging of large lists of keys. The result set requested starts from the
   *   marker. If not provided, then the list starts from the first key.
   */
  function listRecusively (continuationToken) {
    options.continuationToken = continuationToken;

    self.listDirectoryPage(
      options,
      function (error, nextContinuationToken, s3Objects, commonPrefixes) {
        if (error) {
          return callback(error);
        }

        // Each common prefix is only returned once, even across requests using
        // continuation tokens for 'subdirectories' containing many files.
        _.each(commonPrefixes, function (commonPrefix) {
          var subDirectoryOptions = _.chain(
            {}
          ).extend(
            options,
            {
              prefix: commonPrefix
            }
          ).omit(
            'continuationToken'
          ).value();

          self.sendToQueue(subDirectoryOptions);
        });

        // Send any S3 object definitions to be piped onwards.
        s3Objects.forEach(function (object) {
          object.Bucket = options.bucket;
          self.push(object);
        });

        // If there are more objects, go get them.
        if (nextContinuationToken) {
          listRecusively(nextContinuationToken);
        }
        else {
          callback();
        }
      }
    );
  }

  // Start the recursive listing at the beginning, with no continuationToken.
  listRecusively();
};

/**
 * List objects from S3 and push them to the stream, according to the details
 * in the incoming object.
 *
 * @param {Object} options
 * @param {AWS.S3} options.s3Client An AWS client instance.
 * @param {String} options.bucket The bucket to list.
 * @param {Number} [options.maxConcurrency] The limit on concurrent API
 *   requests. Defaults to 10.
 * @param {Number} [options.delimiter] Used to find common prefixes to split out
 *   requests for object listing. Defaults to '/'.
 * @param {Number} [options.maxKeys] Maximum number of keys to return per
 *   request. Defaults to 1000.
 * @param {String} [options.prefix] If present, only list objects with keys that
 *   match the prefix.
 * @param {String} encoding Irrelevant since this is an object stream.
 * @param {Function} callback Invoked after this listing is processed.
 */
S3ConcurrentListObjectStream.prototype.processIncomingObject = function (
  options,
  callback
) {
  if (!options || typeof options !== 'object') {
    return callback(new Error('An object is expected.'));
  }
  if (!options.s3Client) {
    return callback(new Error('Missing options.s3Client'));
  }
  if (!options.bucket) {
    return callback(new Error('Missing options.bucket'));
  }

  options.delimiter = options.delimiter || '/';

  // Reset the global state for the next item.
  //
  // Since incoming items are processed in order, the next not starting until
  // the callback for the first invoked, it is safe to manipulate the global
  // state in this way.
  this.commonPrefixes = {};
  this.queue.drain = _.once(callback);

  // Start things going.
  this.sendToQueue(options);
};

//---------------------------------------------------------------------------
// Overridden methods.
//---------------------------------------------------------------------------

/**
 * Implementation of the necessary transform method.
 *
 * @param {Object} data A listObjects configuration object since this is an
 *   object stream.
 * @param {String} encoding Irrelevant since this is an object stream.
 * @param {Function} callback Invoked after this listing is processed.
 */
S3ConcurrentListObjectStream.prototype._transform = function (
  data,
  encoding,
  callback
) {
  this.processIncomingObject(data, callback);
};

//---------------------------------------------------------------------------
// Export class constructor.
//---------------------------------------------------------------------------

module.exports = S3ConcurrentListObjectStream;
