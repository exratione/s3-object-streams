/**
 * @fileOverview Tests for lib/stream/s3ConcurrentListObjectStream.
 */

// NPM.
var AWS = require('aws-sdk');
var _ = require('lodash');

// Local.
var S3ConcurrentListObjectStream = require('../../../lib/stream/s3ConcurrentListObjectStream');

describe('lib/stream/s3ConcurrentListObjectStream', function () {
  var listObjectsResponse1;
  var listObjectsResponse2;
  var prefix;
  var sandbox;
  var s3Client;
  var s3ConcurrentListObjectStream;

  beforeEach(function () {
    sandbox = sinon.sandbox.create();

    s3Client = new AWS.S3();
    prefix = 'prefix/';

    listObjectsResponse1 = {
      IsTruncated: true,
      NextContinuationToken: 'token',
      CommonPrefixes: [
        { Prefix: prefix + 'b/' },
        { Prefix: prefix + 'c/' }
      ],
      Contents: [
        {
          Key: prefix + 'a1'
        },
        {
          Key: prefix + 'a2'
        }
      ]
    };
    listObjectsResponse2 = {
      IsTruncated: false,
      // The prefixes listed already won't appear again.
      CommonPrefixes: [],
      Contents: [
        {
          Key: prefix + 'a3'
        }
      ]
    };

    sandbox.stub(s3Client, 'listObjectsV2');
    s3Client.listObjectsV2.onCall(0).yields(null, listObjectsResponse1);
    s3Client.listObjectsV2.onCall(1).yields(null, listObjectsResponse2);

    s3ConcurrentListObjectStream = new S3ConcurrentListObjectStream();
    sandbox.stub(s3ConcurrentListObjectStream, 'push');
  });

  afterEach(function () {
    sandbox.restore();
  });

  describe('listDirectoryPage', function () {
    var options;

    beforeEach(function () {
      options = {
        s3Client: s3Client,
        bucket: 'bucket',
        delimiter: '/',
        prefix: 'prefix/',
        continuationToken: 'token',
        maxKeys: 50
      };
    });

    it('functions as expected', function (done) {
      s3ConcurrentListObjectStream.listDirectoryPage(
        options,
        function (error, nextContinuationToken, s3Objects, commonPrefixes) {
          sinon.assert.calledWith(
            s3Client.listObjectsV2,
            {
              Bucket: options.bucket,
              Delimiter: options.delimiter,
              ContinuationToken: options.continuationToken,
              MaxKeys: options.maxKeys,
              Prefix: options.prefix
            },
            sinon.match.func
          );

          expect(nextContinuationToken).to.eql(listObjectsResponse1.NextContinuationToken);
          expect(commonPrefixes).to.eql(_.map(listObjectsResponse1.CommonPrefixes, function (obj) {
            return obj.Prefix;
          }));
          expect(s3Objects).to.eql(listObjectsResponse1.Contents);

          done(error);
        }
      );
    });

    it('yields error on API error', function (done) {
      s3Client.listObjectsV2.onCall(0).yields(new Error());
      s3Client.listObjectsV2.onCall(1).yields(new Error());
      s3Client.listObjectsV2.onCall(2).yields(new Error());

      s3ConcurrentListObjectStream.listDirectoryPage(
        options,
        function (error, nextContinuationToken, commonPrefixes) {
          sinon.assert.callCount(s3Client.listObjectsV2, 3);
          expect(error).to.be.instanceOf(Error);
          done();
        }
      );
    });
  });

  describe('listDirectoryAndRecuse', function () {
    var options;

    beforeEach(function () {
      options = {
        s3Client: s3Client,
        bucket: 'bucket',
        delimiter: '/',
        prefix: prefix,
        maxKeys: 50
      };

      sandbox.stub(s3ConcurrentListObjectStream.queue, 'push');
    });

    it('functions as expected', function (done) {
      s3ConcurrentListObjectStream.listDirectoryAndRecuse(
        options,
        function (error) {
          sinon.assert.callCount(s3Client.listObjectsV2, 2);

          expect(s3Client.listObjectsV2.getCall(0).args[0]).to.eql({
            Bucket: options.bucket,
            Delimiter: options.delimiter,
            ContinuationToken: undefined,
            MaxKeys: options.maxKeys,
            Prefix: options.prefix
          });
          expect(s3Client.listObjectsV2.getCall(1).args[0]).to.eql({
            Bucket: options.bucket,
            Delimiter: options.delimiter,
            ContinuationToken: listObjectsResponse1.NextContinuationToken,
            MaxKeys: options.maxKeys,
            Prefix: options.prefix
          });

          sinon.assert.callCount(s3ConcurrentListObjectStream.push, 3);

          expect(s3ConcurrentListObjectStream.push.getCall(0).args).to.eql([
            listObjectsResponse1.Contents[0]
          ]);
          expect(s3ConcurrentListObjectStream.push.getCall(1).args).to.eql([
            listObjectsResponse1.Contents[1]
          ]);
          expect(s3ConcurrentListObjectStream.push.getCall(2).args).to.eql([
            listObjectsResponse2.Contents[0]
          ]);

          sinon.assert.callCount(s3ConcurrentListObjectStream.queue.push, 2);

          expect(s3ConcurrentListObjectStream.queue.push.getCall(0).args[0]).to.eql({
            s3Client: s3Client,
            bucket: 'bucket',
            delimiter: '/',
            prefix: prefix + 'b/',
            maxKeys: 50
          });
          expect(s3ConcurrentListObjectStream.queue.push.getCall(1).args[0]).to.eql({
            s3Client: s3Client,
            bucket: 'bucket',
            delimiter: '/',
            prefix: prefix + 'c/',
            maxKeys: 50
          });

          done(error);
        }
      );
    });

    it('retries and yields error on API error', function (done) {
      s3Client.listObjectsV2.onCall(0).yields(new Error());
      s3Client.listObjectsV2.onCall(1).yields(new Error());
      s3Client.listObjectsV2.onCall(2).yields(new Error());

      s3ConcurrentListObjectStream.listDirectoryAndRecuse(
        options,
        function (error, commonPrefixes) {
          sinon.assert.callCount(s3Client.listObjectsV2, 3);
          expect(error).to.be.instanceOf(Error);
          done();
        }
      );
    });
  });

  describe('processIncomingObject', function () {
    var options;

    beforeEach(function () {
      options = {
        s3Client: s3Client,
        bucket: 'bucket',
        prefix: prefix
      };

      sandbox.stub(s3ConcurrentListObjectStream.queue, 'push');
    });

    it('functions as expected', function (done) {
      s3ConcurrentListObjectStream.processIncomingObject(options, function (error) {
        done(error);
      });

      sinon.assert.calledWith(
        s3ConcurrentListObjectStream.queue.push,
        sinon.match.object,
        sinon.match.func
      );

      var argOpts = s3ConcurrentListObjectStream.queue.push.getCall(0).args[0];
      expect(argOpts.s3Client).to.equal(options.s3Client);
      expect(argOpts.bucket).to.equal(options.bucket);
      expect(argOpts.delimiter).to.equal('/');
      expect(argOpts.prefix).to.equal(options.prefix);

      s3ConcurrentListObjectStream.queue.drain();
    });

    it('yields error for missing options', function (done) {
      s3ConcurrentListObjectStream.processIncomingObject(null, function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });
    });

    it('yields error for missing options.s3Client', function (done) {
      delete options.s3Client;

      s3ConcurrentListObjectStream.processIncomingObject(options, function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });
    });

    it('yields error for missing options.bucket', function (done) {
      delete options.bucket;

      s3ConcurrentListObjectStream.processIncomingObject(options, function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });
    });
  });
});
