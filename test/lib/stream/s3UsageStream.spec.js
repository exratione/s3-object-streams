/**
 * @fileOverview Tests for lib/stream/s3UsageStream.
 */

// Local.
var constants = require('../../../lib/constants');
var S3UsageStream = require('../../../lib/stream/s3UsageStream');

describe('lib/stream/S3UsageStream', function () {
  var sandbox;
  var s3Objects;
  var usageObjects;
  var s3UsageStream;

  beforeEach(function () {
    sandbox = sinon.sandbox.create();

    s3UsageStream = new S3UsageStream({
      delimiter: '/',
      depth: 2,
      outputFactor: 1
    });

    s3Objects = [
      {
        Bucket: 'bucket',
        Key: 'a/b/c/1',
        Size: 10,
        StorageClass: constants.storageClass.STANDARD
      },
      {
        Bucket: 'bucket',
        Key: 'a/b/c/2',
        Size: 20,
        StorageClass: constants.storageClass.STANDARD
      },
      {
        Bucket: 'bucket',
        Key: 'a/b/c/3',
        Size: 30,
        StorageClass: constants.storageClass.GLACIER
      }
    ];
    usageObjects = [
      [
        {
          path: 'bucket',
          storageClass: {
            STANDARD: {
              count: 1,
              size: 10
            },
            STANDARD_IA: {
              count: 0,
              size: 0
            },
            REDUCED_REDUNDANCY: {
              count: 0,
              size: 0
            },
            GLACIER: {
              count: 0,
              size: 0
            }
          }
        },
        {
          path: 'bucket/a',
          storageClass: {
            STANDARD: {
              count: 1,
              size: 10
            },
            STANDARD_IA: {
              count: 0,
              size: 0
            },
            REDUCED_REDUNDANCY: {
              count: 0,
              size: 0
            },
            GLACIER: {
              count: 0,
              size: 0
            }
          }
        },
        {
          path: 'bucket/a/b',
          storageClass: {
            STANDARD: {
              count: 1,
              size: 10
            },
            STANDARD_IA: {
              count: 0,
              size: 0
            },
            REDUCED_REDUNDANCY: {
              count: 0,
              size: 0
            },
            GLACIER: {
              count: 0,
              size: 0
            }
          }
        }
      ],
      [
        {
          path: 'bucket',
          storageClass: {
            STANDARD: {
              count: 2,
              size: 30
            },
            STANDARD_IA: {
              count: 0,
              size: 0
            },
            REDUCED_REDUNDANCY: {
              count: 0,
              size: 0
            },
            GLACIER: {
              count: 0,
              size: 0
            }
          }
        },
        {
          path: 'bucket/a',
          storageClass: {
            STANDARD: {
              count: 2,
              size: 30
            },
            STANDARD_IA: {
              count: 0,
              size: 0
            },
            REDUCED_REDUNDANCY: {
              count: 0,
              size: 0
            },
            GLACIER: {
              count: 0,
              size: 0
            }
          }
        },
        {
          path: 'bucket/a/b',
          storageClass: {
            STANDARD: {
              count: 2,
              size: 30
            },
            STANDARD_IA: {
              count: 0,
              size: 0
            },
            REDUCED_REDUNDANCY: {
              count: 0,
              size: 0
            },
            GLACIER: {
              count: 0,
              size: 0
            }
          }
        }
      ],
      [
        {
          path: 'bucket',
          storageClass: {
            STANDARD: {
              count: 2,
              size: 30
            },
            STANDARD_IA: {
              count: 0,
              size: 0
            },
            REDUCED_REDUNDANCY: {
              count: 0,
              size: 0
            },
            GLACIER: {
              count: 1,
              size: 30
            }
          }
        },
        {
          path: 'bucket/a',
          storageClass: {
            STANDARD: {
              count: 2,
              size: 30
            },
            STANDARD_IA: {
              count: 0,
              size: 0
            },
            REDUCED_REDUNDANCY: {
              count: 0,
              size: 0
            },
            GLACIER: {
              count: 1,
              size: 30
            }
          }
        },
        {
          path: 'bucket/a/b',
          storageClass: {
            STANDARD: {
              count: 2,
              size: 30
            },
            STANDARD_IA: {
              count: 0,
              size: 0
            },
            REDUCED_REDUNDANCY: {
              count: 0,
              size: 0
            },
            GLACIER: {
              count: 1,
              size: 30
            }
          }
        }
      ],
    ];

    // The s3UsageStream emits an extra duplicate of the final event on the end
    // event for outputFactor 1.
    usageObjects.push(usageObjects[2]);
  });

  afterEach(function () {
    sandbox.restore();
  });

  describe('updateTotals', function () {

    beforeEach(function () {
      sandbox.stub(s3UsageStream, 'push');
    });

    it('functions as expected for outputFactor 1 and default depth, delimiter', function (done) {
      s3UsageStream = new S3UsageStream({
        outputFactor: 1
      });
      sandbox.stub(s3UsageStream, 'push');

      s3UsageStream.updateTotals(s3Objects[0], function (error) {
        sinon.assert.calledWith(
          s3UsageStream.push,
          [
            {
              path: 'bucket',
              storageClass: {
                STANDARD: {
                  count: 1,
                  size: 10
                },
                STANDARD_IA: {
                  count: 0,
                  size: 0
                },
                REDUCED_REDUNDANCY: {
                  count: 0,
                  size: 0
                },
                GLACIER: {
                  count: 0,
                  size: 0
                }
              }
            }
          ]
        );

        done(error);
      });
    });

    it('functions as expected for outputFactor 2, delimiter /, depth 2', function (done) {
      s3UsageStream = new S3UsageStream({
        depth: 2,
        outputFactor: 2
      });
      sandbox.stub(s3UsageStream, 'push');

      s3UsageStream.updateTotals(s3Objects[0], function (error) {
        sinon.assert.notCalled(s3UsageStream.push);

        s3UsageStream.updateTotals(s3Objects[1], function (error) {
          sinon.assert.calledWith(
            s3UsageStream.push,
            usageObjects[1]
          );
          done(error);
        });
      });
    });

    it('functions as expected for outputFactor 1, delimiter /, depth 2', function (done) {
      s3UsageStream.updateTotals(s3Objects[0], function (error) {
        sinon.assert.calledWith(
          s3UsageStream.push,
          usageObjects[0]
        );
        done();
      });
    });

    it('yields error for unrecognized storage class', function (done) {
      s3Objects[0].StorageClass = 'UNKNOWN';

      s3UsageStream.updateTotals(s3Objects[0], function (error) {
        expect(error).to.be.instanceOf(Error);
        done();
      });

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
