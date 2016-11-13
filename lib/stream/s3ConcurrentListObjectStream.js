/**
 * @fileOverview S3ConcurrentListObjectStream class definition.
 */

// Core.
var util = require('util');

// NPM.
var async = require('async');
var _ = require('lodash');

// Local.
var S3ListObjectStream = require('./s3ListObjectStream');

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
 * Further a bad choice of key format and prefix may mean that there is little
 * concurrency. Common prefixes are used to split out requests to different
 * key sets in the bucket, so this works best for buckets with a lot of top
 * level "folders".
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
 * @param {Object} options Standard stream options.
 */
function S3ConcurrentListObjectStream (options) {
  options = options || {};
  // Important; make this an object stream.
  options.objectMode = true;

  S3ConcurrentListObjectStream.super_.call(this, options);
}

util.inherits(S3ConcurrentListObjectStream, S3ListObjectStream);

//---------------------------------------------------------------------------
// Methods
//---------------------------------------------------------------------------

/**
 * List one page of common prefixes from the specified bucket. If there are
 * objects at this level, then return them too.
 *
 * If providing a prefix, only keys matching the prefix will be returned.
 *
 * If providing a marker, list a page of keys starting from the marker
 * position. Otherwise return the first page of keys.
 *
 * If there are
 *
 * @param {Object} options
 * @param {AWS.S3} options.s3Client An AWS client instance.
 * @param {String} options.bucket The bucket name.
 * @param {Number} options.delimiter Used to find common prefixes to split out
 *   requests for object listing. Defaults to '/'.
 * @param {String} [options.prefix] If set only return keys beginning with
 *   the prefix value.
 * @param {String} [options.marker] If set the list only a paged set of keys
 *   starting from the marker.
 * @param {Number} [options.maxKeys] Maximum number of keys to return per
 *   request. Defaults to 1000.
 * @param {Function} callback - Callback of the form
    function (error, nextMarker, Object[], String[]).
 */
S3ConcurrentListObjectStream.prototype.listCommonPrefixesPage = function (
  options,
  callback
) {
  var params = {
    Bucket: options.bucket,
    Delimiter: options.delimiter,
    Marker: options.marker,
    MaxKeys: options.maxKeys,
    Prefix: options.prefix
  };

  // S3 operations have a small but significant error rate.
  async.retry(
    3,
    function (asyncCallback) {
      options.s3Client.listObjects(params, asyncCallback);
    },
    function (error, response) {
      var nextMarker;

      if (error) {
        return callback(error);
      }

      // Check to see if there are yet more common prefixes to be obtained, and if
      // so return the marker for use in the next request.
      if (response.IsTruncated) {
        nextMarker = response.NextMarker;
      }

      callback(null, nextMarker, response.Contents, response.CommonPrefixes);
    }
  );
};

/**
 * Obtain common key prefixes that can then be used to split up the process of
 * listing a bucket, and run it concurrently.
 *
 * This can work very well, or it can work terribly - that strongly depends on
 * the layout of the bucket.
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
 * @param {Function} callback Of the form function (error, String[]).
 */
S3ConcurrentListObjectStream.prototype.listCommonPrefixes = function (
  options,
  callback
) {
  var self = this;
  var commonPrefixes = [];

  /**
   * Recursively list common prefixes.
   *
   * @param {String|undefined} marker A value provided by the S3 API to enable
   *   paging of large lists of keys. The result set requested starts from the
   *   marker. If not provided, then the list starts from the first key.
   */
  function listRecusively (marker) {
    options.marker = marker;

    self.listCommonPrefixesPage(
      options,
      function (error, nextMarker, s3Objects, commonPrefixesPage) {
        if (error) {
          return callback(error);
        }

        commonPrefixes = commonPrefixes.concat(commonPrefixesPage);

        // Send any S3 object definitions to be piped onwards.
        s3Objects.forEach(function (object) {
          object.Bucket = options.bucket;
          self.push(object);
        });

        if (nextMarker) {
          listRecusively(nextMarker);
        }
        else {
          callback(null, _.map(commonPrefixes, function (prefixObj) {
            return prefixObj.Prefix;
          }));
        }
      }
    );
  }

  // Start the recursive listing at the beginning, with no marker.
  listRecusively();
};

/**
 * List objects from S3 and push them to the stream.
 *
 * @param {Object} options
 * @param {AWS.S3} options.s3Client An AWS client instance.
 * @param {String} options.bucket The bucket to list.
 * @param {String[]} options.commonPrefixes The common prefixes used to generate
 *   multiple requests.
 * @param {Number} options.maxConcurrency The limit on concurrent API requests.
 * @param {Number} options.delimiter Used to find common prefixes to split out
 *   requests for object listing. Defaults to '/'.
 * @param {Number} options.maxKeys Maximum number of keys to return per
 *   request. Defaults to 1000.
 * @param {String} [options.prefix] If present, only list objects with keys that
 *   match the prefix.
 * @param {String} encoding Irrelevant since this is an object stream.
 * @param {Function} callback Invoked after this listing is processed.
 */
S3ConcurrentListObjectStream.prototype.listObjectsConcurrently = function (
  options,
  callback
) {
  var self = this;
  var tasks = _.map(options.commonPrefixes, function (commonPrefix) {
    return function (asyncCallback) {
      // Create a new options object with just the items we want.
      var taskOptions = _.omit(options, [
        'commonPrefixes',
        'maxConcurrency'
      ]);

      // Common prefixes returned by the AWS API will include options.prefix,
      // so simply replace the prefix option.
      taskOptions.prefix = commonPrefix;

      self.listObjects(taskOptions, asyncCallback);
    };
  });

  async.parallelLimit(
    tasks,
    options.maxConcurrency,
    callback
  );
};

/**
 * List objects from S3 and push them to the stream.
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
  var self = this;

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
  options.maxConcurrency = options.maxConcurrency || 10;
  options.maxKeys = options.maxKeys || 1000;

  async.series({
    listCommonPrefixes: function (asyncCallback) {
      self.listCommonPrefixes(options, function (error, listedCommonPrefixes) {
        options.commonPrefixes = listedCommonPrefixes;
        asyncCallback(error);
      });
    },
    listObjectsConcurrently: function (asyncCallback) {
      self.listObjectsConcurrently(options, asyncCallback);
    }
  }, function (error) {
    callback(error);
  });
};


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
