const { spawn } = require("child_process");
const dynamoose = require("dynamoose");
require("dotenv").config();
const { parseArgsStringToArgv } = require("string-argv");
const fsp = require("fs").promises;
const path = require("path");
const fs = require("fs");
var AWS = require("aws-sdk");

let currentResolution = 0;
const resolutionOptions = [
  { preset: "ultrafast", crf: 28, segmentSizeInMB: "500K" },
  { preset: "veryfast", crf: 28, segmentSizeInMB: "1M" },
  { preset: "veryslow", crf: 22, segmentSizeInMB: "2M" },
  { preset: "veryslow", crf: 28, segmentSizeInMB: "2M" },
];
AWS.config.update({
  region: process.env.REGION,
  credentials: {
    accessKeyId: process.env.KEY_ID,
    secretAccessKey: process.env.KEY_SECRET,
  },
});
var meta = new AWS.MetadataService();
const s3 = new AWS.S3({
  region: process.env.REGION,
  credentials: {
    accessKeyId: process.env.KEY_ID,
    secretAccessKey: process.env.KEY_SECRET,
  },
});

const ddb = new dynamoose.aws.ddb.DynamoDB({
  credentials: {
    accessKeyId: process.env.KEY_ID,
    secretAccessKey: process.env.KEY_SECRET,
  },
  region: process.env.REGION,
});
dynamoose.aws.ddb.set(ddb);

const ec2Schema = new dynamoose.Schema({
  Ec2Id: {
    type: String,
    required: true,
    hashKey: true,
  },
  data: {
    type: Object,
    schema: {
      inputBucket: String,
      outPutBucket: String,
      fileName: String,
      outputFolder: String,
    },
  },
});
Ec2Table = dynamoose.model(process.env.DYNAMODB_TABLE, ec2Schema);
function main() {
  // const Ec2Id = "i-03a298f717f7ff068";
  meta.request("/latest/meta-data/instance-id", async function (err, Ec2Id) {
    const scriptData = await Ec2Table.get(Ec2Id);
    console.log(scriptData);
    const filename = scriptData.data.fileName;
    const bucketName = scriptData.data.inputBucket;
    const destinyBucket = scriptData.data.outPutBucket;
    try {
      const readStream = s3
        .getObject({ Bucket: bucketName, Key: filename })
        .createReadStream();

      const writeStream = fs.createWriteStream("./" + filename);
      console.log("Writing S3 file on EBS...");
      readStream.pipe(writeStream);
      writeStream.on("finish", () => {
        console.log("Finished process.\nStarting ffmpeg conversion...");
        hlsConvert({ filename, destinyBucket, bucketName, Ec2Id });
      });
      let bytes = 0;
      readStream.on("data", (data) => {
        bytes += data.byteLength;
        console.log("Downoaded " + (bytes / 1000000).toFixed(3) + "MB");
      });
    } catch (error) {
      console.log("Error on function main, terminating instance...");
      console.log(error);
      await s3
        .deleteObject({
          Bucket: bucketName,
          Key: filename,
        })
        .promise();
      console.log("Deleting dynamoDb document for shuting down instance...");
      // remover item do dynamodb
      await Ec2Table.delete(Ec2Id);
    }
  });
}

async function hlsConvert({ filename, destinyBucket, bucketName, Ec2Id }) {
  try {
    fs.mkdirSync("./hls");
    let args = parseArgsStringToArgv(`ffmpeg -i ${filename} \
  -map 0:v:0 -map 0:a:0 \
  -map 0:v:0 -map 0:a:0 \
  -map 0:v:0 -map 0:a:0 \
  -map 0:v:0 -map 0:a:0 \
  -map 0:v:0 -map 0:a:0 \
	-map 0:v:0 -map 0:a:0 \
  -c:v libx264 -crf ${resolutionOptions[currentResolution].crf} -c:a aac -ar 48000 \
	-filter:v:0 scale=w=256:h=144  -maxrate:v:0 214k -b:a:0 32k \
	-filter:v:1 scale=w=426:h=240  -maxrate:v:1 428k -b:a:1 64k \
  -filter:v:2 scale=w=480:h=360  -maxrate:v:2 700k -b:a:2 96k \
  -filter:v:3 scale=w=640:h=480  -maxrate:v:3 1250k -b:a:3 128k \
  -filter:v:4 scale=w=1280:h=720 -maxrate:v:4 2500k -b:a:4 128k \
  -filter:v:5 scale=w=1920:h=1080 -maxrate:v:5 4500k -b:a:5 192k \
  -var_stream_map "v:0,a:0,name:144p v:1,a:1,name:240p v:2,a:2,name:360p v:3,a:3,name:480p v:4,a:4,name:720p v:5,a:5,name:1080p" \
  -preset ${resolutionOptions[currentResolution].preset} -cpu-used 12 -threads 4 -f hls \
  -hls_segment_size ${resolutionOptions[currentResolution].segmentSizeInMB} -hls_list_size 0 -hls_flags  independent_segments  \
  -master_pl_name "auto.m3u8" \
  -y "./hls/%v.m3u8"`);
    let cmd = args.shift();
    const script = spawn(cmd, args);
    script.stdout.setEncoding("utf8");
    script.stderr.setEncoding("utf8");
    script.stdout.on("data", function (data) {
      console.log(data);
    });
    script.stderr.on("data", function (data) {
      console.log(data);
    });
    script.on("exit", async function (code) {
      console.log({ code });
      console.log("Conversion Finished.\nStarting revision...");
      const files = await fsp.readdir("./hls");
      let biggerSize = 0;
      // se der maior que 5mb fazer com 15s apagando o que ja existe
      for (const file of files) {
        const videoPath = path.resolve(__dirname, "./hls", file);
        const data = await fsp.readFile(videoPath);
        if (data.byteLength > biggerSize) {
          console.log("Bigger file found until now:");
          console.log(file);
          console.log(data.byteLength);
          biggerSize = data.byteLength;
        }
      }
      if (biggerSize >= 5500000) {
        for (const file of files) {
          await fsp.unlink(path.resolve(__dirname, "./hls", file));
        }
        await fsp.rmdir(path.resolve(__dirname, "./hls"));
        if (currentResolution == resolutionOptions.length - 1) {
          console.log("Deleting input object...");
          await s3
            .deleteObject({
              Bucket: bucketName,
              Key: filename,
            })
            .promise();
          console.log(
            "Deleting dynamoDb document for shuting down instance..."
          );
          // TODO: ENVIAR ERRRO VIA EMAIL
          // remover item do dynamodb
          await Ec2Table.delete(Ec2Id);
        }
        currentResolution++;
        console.log("Revision Faild.\nRetrying with new resolution....\n");
        console.log(resolutionOptions[currentResolution]);
        return hlsConvert({ filename, destinyBucket, bucketName, Ec2Id });
      }
      console.log("Revision Finished.\nSending files to s3 output bucket");
      let sentFilescounter = 0;
      for (const file of files) {
        sentFilescounter++;
        await s3
          .putObject({
            Bucket: destinyBucket,
            Key: filename.split(".")[0] + "/" + file,
            Body: fs.readFileSync("./hls/" + file),
          })
          .promise();
        console.log(sentFilescounter + " of " + files.length + " sent.");
      }
      console.log("Deleting input object...");
      await s3
        .deleteObject({
          Bucket: bucketName,
          Key: filename,
        })
        .promise();
      console.log("Deleting dynamoDb document for shuting down instance...");
      // remover item do dynamodb
      await Ec2Table.delete(Ec2Id);
    });
  } catch (error) {
    console.log("Error on function main, terminating instance...");
    console.log(error);
    await s3
      .deleteObject({
        Bucket: bucketName,
        Key: filename,
      })
      .promise();
    console.log("Deleting dynamoDb document for shuting down instance...");
    // remover item do dynamodb
    await Ec2Table.delete(Ec2Id);
  }
}
main();
