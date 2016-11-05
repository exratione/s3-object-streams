/**
 * @fileOverview Tests for lib/stream/s3UsageStream.
 */

// Local.
var S3UsageStream = require('../../../lib/stream/s3UsageStream');

describe('lib/stream/S3UsageStream', function () {
  var s3Objects;
  var usageObjects;
  var s3UsageStream;

  beforeEach(function () {
    s3UsageStream = new S3UsageStream({
      delimiter: '/',
      depth: 2
    });

    s3Objects = [
      {
        Bucket: 'bucket',
        Key: 'a/b/c/d/1',
        Size: 10
      },
      {
        Bucket: 'bucket',
        Key: 'a/b/c/d/2',
        Size: 20
      },
      {
        Bucket: 'bucket',
        Key: 'a/b/c/d/3',
        Size: 30
      }
    ];
    usageObjects = [
      [
        {
          path: 'bucket',
          count: 1,
          size: 10
        },
        {
          path: 'bucket/a',
          count: 1,
          size: 10
        },
        {
          path: 'bucket/a/b',
          count: 1,
          size: 10
        }
      ],
      [
        {
          path: 'bucket',
          count: 2,
          size: 30
        },
        {
          path: 'bucket/a',
          count: 2,
          size: 30
        },
        {
          path: 'bucket/a/b',
          count: 2,
          size: 30
        }
      ],
      [
        {
          path: 'bucket',
          count: 3,
          size: 60
        },
        {
          path: 'bucket/a',
          count: 3,
          size: 60
        },
        {
          path: 'bucket/a/b',
          count: 3,
          size: 60
        }
      ]
    ];
  });

  describe('updateTotals', function () {
    var totals;

    it('functions as expected for default depth, delimiter', function () {
      s3UsageStream = new S3UsageStream();
      totals = s3UsageStream.updateTotals(s3Objects[0]);
      expect(totals).to.eql([
        {
          path: 'bucket',
          count: 1,
          size: 10
        }
      ]);
    });

    it('functions as expected for delimiter /, depth 2', function () {
      totals = s3UsageStream.updateTotals(s3Objects[0]);
      expect(totals).to.eql(usageObjects[0]);
    });
  });

  describe('streaming', function () {
    var index;

    beforeEach(function () {
      index = 0;
    });

    it('in non-flowing mode', function (done) {
      s3UsageStream.on('readable', function () {
        var usageObject;

        do {
          // Don't pass a size value to read, as an object stream always returns
          // one object from a read request.
          usageObject = s3UsageStream.read();
          if (usageObject) {
            expect(usageObjects[index]).to.eql(usageObject);
            index++;
          }
        } while (usageObject);
      });

      s3UsageStream.on('end', function () {
        // We should only see the end event emitted at the end, after running
        // through all of the S3 objects.
        expect(index).to.equal(usageObjects.length);
        done();
      });

      s3Objects.forEach(function (s3Object) {
        s3UsageStream.write(s3Object);
      });
      s3UsageStream.end();
    });

    it('in flowing mode', function (done) {
      s3UsageStream.on('data', function (usageObject) {
        expect(usageObjects[index]).to.eql(usageObject);
        index++;
      });

      s3UsageStream.on('end', function () {
        // We should only see the end event emitted at the end, after running
        // through all of the S3 objects.
        expect(index).to.equal(usageObjects.length);
        done();
      });

      s3Objects.forEach(function (s3Object) {
        s3UsageStream.write(s3Object);
      });
      s3UsageStream.end();
    });

    it('emits error for null S3 object', function (done) {
      s3UsageStream.on('error', function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });

      s3UsageStream.write(null);
    });

    it('emits error for S3 object without size', function (done) {
      s3UsageStream.on('error', function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });

      s3UsageStream.write({});
    });
  });
});
