const os = require("oci-objectstorage");
const common = require("oci-common");
const st = require("oci-streaming");
const fs = require("fs");

var express = require('express');
var router = express.Router();

const compartmentId = "ocid1.compartment.oc1..aaaaaaaas56gltnrdtw6vpkai26gyssa2i6udyw5vlfp27iw2biaw2t5yz5q";

const provider = new common.ConfigFileAuthenticationDetailsProvider();

const client = new os.ObjectStorageClient({
  authenticationDetailsProvider: provider
});

const streamAdminClient = new st.StreamAdminClient({ authenticationDetailsProvider: provider });
const streamClient = new st.StreamClient({ authenticationDetailsProvider: provider });
const waiters = streamAdminClient.createWaiters();

async function getNamespace() {
  const request = {}
  const out = await client.getNamespace(request)
  console.log("out is ")
  console.log(out)

  return out.value
}

router.get('/os/bucket/:name', async function (req, res, next) {
  const bucketName = req.params.name
  const namespace = await getNamespace()
  console.log("Creating bucket with name " + bucketName)

  const bucketDetails = {
    name: bucketName,
    compartmentId: compartmentId
  };

  const createBucketRequest = {
    namespaceName: namespace,
    createBucketDetails: bucketDetails
  }

  const createBucketResponse = await client.createBucket(createBucketRequest);
  console.log("Create Bucket executed successfully" + createBucketResponse);

  return res.json({"message": "Created bucket successfully"})
})

router.get('/os/upload/:bucketName/:objectName', async function (req, res, next) {
  const bucketName = req.params.bucketName
  const objectName = req.params.objectName
  const fileName = req.headers.filename
  const namespace = await getNamespace()

  const stats = fs.statSync(fileName);
  const nodeFsBlob = new os.NodeFSBlob(fileName, stats.size);
  const objectData = await nodeFsBlob.getData();

  console.log("Bucket is created. Now adding object to the Bucket.");
  const putObjectRequest = {
    namespaceName: namespace,
    bucketName: bucketName,
    putObjectBody: objectData,
    objectName: objectName,
    contentLength: stats.size
  };

  const putObjectResponse = await client.putObject(putObjectRequest);

  return res.json({"message": "Object " + objectName + " has been created from file " + fileName})
})

router.get('/st/create/:streamName', async function(req, res, next){
  const streamName = req.params.streamName

  const createStreamDetails = {
    name: streamName,
    compartmentId: compartmentId,
    partitions: 1
  };
  const creatStreamRequest = {
    createStreamDetails: createStreamDetails
  };

  const createStreamResponse = await streamAdminClient.createStream(creatStreamRequest);

  return res.json({"message": "Created stream with name " + streamName})
})

// router.get('/st/publish/:streamName', async function (req, res, next) {
//   const streamName = req.params.streamName
//
//   const listStreamsRequest = {
//     compartmentId: compartmentId,
//     name: streamName,
//     lifecycleState: st.models.Stream.LifecycleState.Active.toString()
//   };
//   const listStreamsResponse = await streamAdminClient.listStreams(listStreamsRequest);
//
//   let streamDetails
//   if (listStreamsResponse.items.length !== 0) {
//     console.log("An active stream with name %s was found.", streamName);
//     const getStreamRequest = {
//       streamId: listStreamsResponse.items[0].id
//     };
//     const getStreamResponse = await streamAdminClient.getStream(getStreamRequest);
//     streamDetails = getStreamResponse.stream;
//   } else {
//     return res.json({"message": "stream with name " + streamName + " not found"})
//   }
//
//   console.log("Publishing to streamId " + streamDetails.id)
//   let streamId = streamDetails.id
//
//   // build up a putRequest and publish some messages to the stream
//   let messages = [];
//   for (let i = 1; i <= 10; i++) {
//     let entry = {
//       key: Buffer.from("messageKey" + i).toString("base64"),
//       value: Buffer.from("messageValue" + i).toString("base64")
//     };
//     messages.push(entry);
//   }
//
//   console.log("Publishing %s messages to stream %s.", messages.length, streamId);
//   const putMessageDetails = { messages: messages };
//   const putMessagesRequest = {
//     putMessagesDetails: putMessageDetails,
//     streamId: streamDetails.messagesEndpoint
//   };
//   const putMessageResponse = await streamClient.putMessages(putMessagesRequest);
//   for (var entry of putMessageResponse.putMessagesResult.entries)
//     console.log("Published messages to partition %s, offset %s", entry.partition, entry.offset);
//
//   return res.json({"message": "published 10 messages to stream"})
//
// })


module.exports = router;
