'use strict';

let AWS = require("aws-sdk");

let payload = '{"Records":[{"eventVersion":"2.0","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2016-11-03T21:07:12.997Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AROAJ45NVQBU3S76WFN5M:i-d9f990e8"},"requestParameters":{"sourceIPAddress":"172.16.4.123"},"responseElements":{"x-amz-request-id":"20EFC10BC4191422","x-amz-id-2":"tbND3KYX2vvHWZNrHBdM8RAfReFoCK4umL+lWOrslKwbYYhKIuQyCIjYsnbkQGNlfBNUczLbT0Y="},"s3":{"s3SchemaVersion":"1.0","configurationId":"0d1537b3-c03a-4d67-b2e8-40a0baaa939e","bucket":{"name":"puse1-ecom-mt-economist-storage","ownerIdentity":{"principalId":"ABJKTWT105KVU"},"arn":"arn:aws:s3:::puse1-ecom-mt-economist-storage"},"object":{"key":"economist/pub/nodes/21709631.json","size":10679,"eTag":"562e7c5b8b915e61e491a5b68ce385c5","sequencer":"00581BA700D5DB7140"}}}]}'

// Mock s3.headObject.
function headObject(options, callback) {
  console.log(options);
  callback(false, {"Metadata": {"cacheTargets": "/lists/decimate"}});
}

// Mock prewarm request.
function prewarmRequest(target) {
  console.log("prewarm request sent for " + target);
}

// Mock context.
let mockContext = {
  done: function(state, message) {
    console.log(message);
  }
}

module.exports.prewarm = (event, context, callback) => {
  let s3 = new AWS.S3({apiVersion: '2006-03-01'});
  let bucket = event.Records[0].s3.bucket.name;
  let key = event.Records[0].s3.object.key;

  console.log(event);
  console.log("preparing prewarm for key " + key);
  // Swap this out for s3.headObject in real implementation.
  headObject({ Bucket: bucket, Key: key}, (err, data) => {
    if (err) {
      console.log("Cannot get object from s3", err.stack);
      return;
    } else {
      let datastr = JSON.stringify(data);
      let message = JSON.stringify(event);
      if (data.hasOwnProperty("Metadata") ){
        let metaData = data.Metadata;
        let cacheTargets = metaData["cacheTargets"]
        // Send prewarm requests to cache targets.
        cacheTargets.split(",").forEach(target => prewarmRequest(target));
      }
    }
  });
  context.done(null, 'cache prewarm complete');
};

// This mocks what would actually happen in a real implementation by sending
// a dummy payload and a mock context.
module.exports.prewarm(JSON.parse(payload), mockContext);
