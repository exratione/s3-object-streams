/**
 * @fileOverview Tests for lib/stream/s3ConcurrentListObjectStream.
 */

// NPM.
var AWS = require('aws-sdk');

// Local.
var S3ConcurrentListObjectStream = require('../../../lib/stream/s3ConcurrentListObjectStream');

describe('lib/stream/s3ConcurrentListObjectStream', function () {
  var listCommonPrefixesResponse1;
  var listCommonPrefixesResponse2;
  var allCommonPrefixes;
  var sandbox;
  var s3Client;
  var s3ConcurrentListObjectStream;

  beforeEach(function () {
    sandbox = sinon.sandbox.create();

    s3Client = new AWS.S3();
    sandbox.stub(s3Client, 'listObjects');

    listCommonPrefixesResponse1 = {
      IsTruncated: true,
      NextMarker: 'marker',
      CommonPrefixes: [
        { Prefix: 'a/z/' },
        { Prefix: 'b/y/' }
      ]
    };
    listCommonPrefixesResponse2 = {
      IsTruncated: false,
      CommonPrefixes: [
        { Prefix: 'c/x/' },
        { Prefix: 'd/w/' }
      ]
    };

    allCommonPrefixes = ['a/z/', 'b/y/', 'c/x/', 'd/w/'];

    s3Client.listObjects.onCall(0).yields(null, listCommonPrefixesResponse1);
    s3Client.listObjects.onCall(1).yields(null, listCommonPrefixesResponse2);

    s3ConcurrentListObjectStream = new S3ConcurrentListObjectStream();
    sandbox.stub(s3ConcurrentListObjectStream, 'listObjects').yields();
  });

  afterEach(function () {
    sandbox.restore();
  });

  describe('listCommonPrefixesPage', function () {
    var options;

    beforeEach(function () {
      options = {
        s3Client: s3Client,
        bucket: 'bucket',
        delimiter: '/',
        prefix: 'prefix',
        marker: 'marker',
        maxKeys: 50
      };
    });

    it('functions as expected', function (done) {
      s3ConcurrentListObjectStream.listCommonPrefixesPage(
        options,
        function (error, nextMarker, commonPrefixes) {
          sinon.assert.calledWith(
            s3Client.listObjects,
            {
              Bucket: options.bucket,
              Delimiter: options.delimiter,
              Marker: options.marker,
              MaxKeys: options.maxKeys,
              Prefix: options.prefix
            },
            sinon.match.func
          );

          expect(nextMarker).to.eql(listCommonPrefixesResponse1.NextMarker);
          expect(commonPrefixes).to.eql(listCommonPrefixesResponse1.CommonPrefixes);

          done(error);
        }
      );
    });

    it('yields error on API error', function (done) {
      s3Client.listObjects.onCall(0).yields(new Error());
      s3Client.listObjects.onCall(1).yields(new Error());
      s3Client.listObjects.onCall(2).yields(new Error());

      s3ConcurrentListObjectStream.listCommonPrefixesPage(
        options,
        function (error, nextMarker, commonPrefixes) {
          sinon.assert.callCount(s3Client.listObjects, 3);
          expect(error).to.be.instanceOf(Error);
          done();
        }
      );
    });
  });

  describe('listCommonPrefixes', function () {
    var options;

    beforeEach(function () {
      options = {
        s3Client: s3Client,
        bucket: 'bucket',
        delimiter: '/',
        prefix: 'prefix',
        maxKeys: 50
      };
    });

    it('functions as expected', function (done) {
      s3ConcurrentListObjectStream.listCommonPrefixes(
        options,
        function (error, commonPrefixes) {
          sinon.assert.callCount(s3Client.listObjects, 2);

          expect(s3Client.listObjects.getCall(0).args[0]).to.eql({
            Bucket: options.bucket,
            Delimiter: options.delimiter,
            Marker: undefined,
            MaxKeys: options.maxKeys,
            Prefix: options.prefix
          });
          expect(s3Client.listObjects.getCall(1).args[0]).to.eql({
            Bucket: options.bucket,
            Delimiter: options.delimiter,
            Marker: listCommonPrefixesResponse1.NextMarker,
            MaxKeys: options.maxKeys,
            Prefix: options.prefix
          });

          expect(commonPrefixes).to.eql(allCommonPrefixes);

          done(error);
        }
      );
    });

    it('retries and yields error on API error', function (done) {
      s3Client.listObjects.onCall(0).yields(new Error());
      s3Client.listObjects.onCall(1).yields(new Error());
      s3Client.listObjects.onCall(2).yields(new Error());

      s3ConcurrentListObjectStream.listCommonPrefixes(
        options,
        function (error, commonPrefixes) {
          sinon.assert.callCount(s3Client.listObjects, 3);
          expect(error).to.be.instanceOf(Error);
          done();
        }
      );
    });
  });

  describe('listObjectsConcurrently', function () {
    var options;

    beforeEach(function () {
      options = {
        s3Client: s3Client,
        bucket: 'bucket',
        commonPrefixes: allCommonPrefixes,
        delimiter: '/',
        prefix: 'prefix',
        maxConcurrency: 10,
        maxKeys: 50
      };
    });

    it('functions as expected', function (done) {
      s3ConcurrentListObjectStream.listObjectsConcurrently(
        options,
        function (error) {
          sinon.assert.callCount(
            s3ConcurrentListObjectStream.listObjects,
            allCommonPrefixes.length
          );

          // Ordering is random, so don't know which call gets which prefix.
          // Check one.
          sinon.assert.calledWith(
            s3ConcurrentListObjectStream.listObjects,
            sinon.match.object,
            sinon.match.func
          );

          var argOpts = s3ConcurrentListObjectStream.listObjects.getCall(0).args[0];
          var possiblePrefixes = [
            'prefix/a/z/',
            'prefix/b/y/',
            'prefix/c/x/',
            'prefix/d/w/',
          ];
          expect(argOpts.s3Client).to.equal(options.s3Client);
          expect(argOpts.bucket).to.equal(options.bucket);
          expect(argOpts.delimiter).to.equal(options.delimiter);
          expect(possiblePrefixes).to.include(argOpts.prefix);
          expect(argOpts.maxKeys).to.equal(options.maxKeys);

          done(error);
        }
      );
    });

    it('deals correctly with prefix/ rather than just prefix', function (done) {
      options.prefix = 'prefix/';

      s3ConcurrentListObjectStream.listObjectsConcurrently(
        options,
        function (error) {
          sinon.assert.callCount(
            s3ConcurrentListObjectStream.listObjects,
            allCommonPrefixes.length
          );

          // Ordering is random, so don't know which call gets which prefix.
          // Check one.
          sinon.assert.calledWith(
            s3ConcurrentListObjectStream.listObjects,
            sinon.match.object,
            sinon.match.func
          );

          var argOpts = s3ConcurrentListObjectStream.listObjects.getCall(0).args[0];
          var possiblePrefixes = [
            'prefix/a/z/',
            'prefix/b/y/',
            'prefix/c/x/',
            'prefix/d/w/',
          ];
          expect(argOpts.s3Client).to.equal(options.s3Client);
          expect(argOpts.bucket).to.equal(options.bucket);
          expect(argOpts.delimiter).to.equal(options.delimiter);
          expect(possiblePrefixes).to.include(argOpts.prefix);
          expect(argOpts.maxKeys).to.equal(options.maxKeys);

          done(error);
        }
      );
    });

    it('yields error on listObjects error', function (done) {
      s3ConcurrentListObjectStream.listObjects.yields(new Error());

      s3ConcurrentListObjectStream.listObjectsConcurrently(
        options,
        function (error) {
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
        prefix: 'prefix'
      };

      sandbox.stub(s3ConcurrentListObjectStream, 'listCommonPrefixes').yields(
        null,
        allCommonPrefixes
      );
      sandbox.stub(s3ConcurrentListObjectStream, 'listObjectsConcurrently').yields();
    });

    it('functions as expected', function (done) {
      s3ConcurrentListObjectStream.processIncomingObject(options, function (error) {

        sinon.assert.calledWith(
          s3ConcurrentListObjectStream.listCommonPrefixes,
          sinon.match.object,
          sinon.match.func
        );

        var argOpts = s3ConcurrentListObjectStream.listCommonPrefixes.getCall(0).args[0];
        expect(argOpts.s3Client).to.equal(options.s3Client);
        expect(argOpts.bucket).to.equal(options.bucket);
        expect(argOpts.delimiter).to.equal(options.delimiter);
        expect(argOpts.maxKeys).to.equal(options.maxKeys);
        expect(argOpts.prefix).to.equal(options.prefix);

        sinon.assert.calledWith(
          s3ConcurrentListObjectStream.listObjectsConcurrently,
          sinon.match.object,
          sinon.match.func
        );

        argOpts = s3ConcurrentListObjectStream.listObjectsConcurrently.getCall(0).args[0];
        expect(argOpts.s3Client).to.equal(options.s3Client);
        expect(argOpts.bucket).to.equal(options.bucket);
        expect(argOpts.commonPrefixes).to.equal(allCommonPrefixes);
        expect(argOpts.delimiter).to.equal(options.delimiter);
        expect(argOpts.maxKeys).to.equal(options.maxKeys);
        expect(argOpts.prefix).to.equal(options.prefix);

        done(error);
      });

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
