/**
 * @fileOverview Tests for lib/stream/s3ListObjectStream.
 */

// NPM.
var AWS = require('aws-sdk');

// Local.
var S3ListObjectStream = require('../../../lib/stream/s3ListObjectStream');

describe('lib/stream/s3ListObjectStream', function () {
  var listObjectResponse1;
  var listObjectResponse2;
  var sandbox;
  var s3Client;
  var s3ListObjectStream;
  var s3Objects;

  beforeEach(function () {
    sandbox = sinon.sandbox.create();

    s3ListObjectStream = new S3ListObjectStream();

    listObjectResponse1 = {
      IsTruncated: true,
      Contents: [
        {
          Key: 'a1'
        },
        {
          Key: 'a2'
        }
      ]
    };

    listObjectResponse2 = {
      IsTruncated: false,
      Contents: [
        {
          Key: 'b1'
        },
        {
          Key: 'b2'
        }
      ]
    };

    s3Objects = [
      {
        Bucket: 'bucket',
        Key: 'a1'
      },
      {
        Bucket: 'bucket',
        Key: 'a2'
      },
      {
        Bucket: 'bucket',
        Key: 'b1'
      },
      {
        Bucket: 'bucket',
        Key: 'b2'
      }
    ];

    s3Client = new AWS.S3();

    sandbox.stub(s3Client, 'listObjects');
    s3Client.listObjects.onCall(0).yields(null, listObjectResponse1);
    s3Client.listObjects.onCall(1).yields(null, listObjectResponse2);
  });

  afterEach(function () {
    sandbox.restore();
  });

  describe('listObjectsPage', function () {
    var options;

    beforeEach(function () {
      options = {
        s3Client: s3Client,
        bucket: 'bucket',
        prefix: 'prefix',
        marker: 'marker',
        maxKeys: 50,
        delimiter: 'delimiter'
      };
    });

    it('functions as expected', function (done) {
      s3ListObjectStream.listObjectsPage(options, function (error) {
        sinon.assert.calledWith(
          s3Client.listObjects,
          {
            Bucket: options.bucket,
            Marker: options.marker,
            MaxKeys: options.maxKeys,
            Prefix: options.prefix,
            Delimiter: options.delimiter
          },
          sinon.match.func
        );

        done(error);
      });
    });

    it('retries on errors and yields errors appropriately', function (done) {
      // Errors to get past the retry.
      s3Client.listObjects.onCall(0).yields(new Error());
      s3Client.listObjects.onCall(1).yields(new Error());
      s3Client.listObjects.onCall(2).yields(new Error());

      s3ListObjectStream.listObjectsPage(options, function (error) {
        sinon.assert.callCount(s3Client.listObjects, 3);
        expect(error).to.be.instanceOf(Error);
        done();
      });
    })
  });

  describe('listObjects', function () {
    var options;

    beforeEach(function () {
      options = {
        s3Client: s3Client,
        bucket: 'bucket',
        prefix: 'prefix',
        delimiter: 'delimiter'
      }
    });

    it('functions as expected', function (done) {
      sandbox.stub(s3ListObjectStream, 'push');

      s3ListObjectStream.listObjects(options, function (error) {
        sinon.assert.callCount(s3Client.listObjects, 2);
        sinon.assert.callCount(s3ListObjectStream.push, 4);

        expect(s3Client.listObjects.getCall(0).args[0]).to.eql({
          Bucket: options.bucket,
          Marker: undefined,
          MaxKeys: 1000,
          Prefix: options.prefix,
          Delimiter: options.delimiter
        });
        expect(s3Client.listObjects.getCall(1).args[0]).to.eql({
          Bucket: options.bucket,
          Marker: listObjectResponse1.Contents[1].Key,
          MaxKeys: 1000,
          Prefix: options.prefix,
          Delimiter: options.delimiter
        });

        expect(s3ListObjectStream.push.getCall(0).args).to.eql([
          listObjectResponse1.Contents[0]
        ]);
        expect(s3ListObjectStream.push.getCall(1).args).to.eql([
          listObjectResponse1.Contents[1]
        ]);
        expect(s3ListObjectStream.push.getCall(2).args).to.eql([
          listObjectResponse2.Contents[0]
        ]);
        expect(s3ListObjectStream.push.getCall(3).args).to.eql([
          listObjectResponse2.Contents[1]
        ]);

        done(error);
      });
    });

    it('yields error for failed API call', function (done) {
      s3Client.listObjects.onCall(0).yields(new Error());
      s3Client.listObjects.onCall(1).yields(new Error());
      s3Client.listObjects.onCall(2).yields(new Error());

      s3ListObjectStream.listObjects(options, function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });
    });

    it('yields error for missing options', function (done) {
      s3ListObjectStream.listObjects(null, function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });
    });

    it('yields error for missing options.s3Client', function (done) {
      delete options.s3Client;

      s3ListObjectStream.listObjects(options, function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });
    });

    it('yields error for missing options.bucket', function (done) {
      delete options.bucket;

      s3ListObjectStream.listObjects(options, function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });
    });
  });

  describe('streaming', function () {
    var index;
    var options;

    beforeEach(function () {
      index = 0;
      options = {
        s3Client: s3Client,
        bucket: 'bucket',
        prefix: 'prefix',
        maxKeys: 50
      };
    });

    it('in non-flowing mode', function (done) {
      s3ListObjectStream.on('readable', function () {
        var s3Object;

        do {
          // Don't pass a size value to read, as an object stream always returns
          // one object from a read request.
          s3Object = s3ListObjectStream.read();
          if (s3Object) {
            expect(s3Objects[index]).to.eql(s3Object);
            index++;
          }
        } while (s3Object);
      });

      s3ListObjectStream.on('end', function () {
        // We should only see the end event emitted at the end, after running
        // through all of the S3 objects.
        expect(index).to.equal(s3Objects.length);
        done();
      });

      s3ListObjectStream.write(options);
      s3ListObjectStream.end();
    });

    it('in flowing mode', function (done) {
      s3ListObjectStream.on('data', function (s3Object) {
        expect(s3Objects[index]).to.eql(s3Object);
        index++;
      });

      s3ListObjectStream.on('end', function () {
        // We should only see the end event emitted at the end, after running
        // through all of the S3 objects.
        expect(index).to.equal(s3Objects.length);
        done();
      });

      s3ListObjectStream.write(options);
      s3ListObjectStream.end();
    });

    it('emits errors appropriately', function (done) {
      s3Client.listObjects.onCall(0).yields(new Error());
      s3Client.listObjects.onCall(1).yields(new Error());
      s3Client.listObjects.onCall(2).yields(new Error());

      s3ListObjectStream.on('error', function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });

      s3ListObjectStream.write(options);
    });
  });

});
