const {MongoClient} = require ( 'mongodb');
const setTimeout = require('timers/promises');
const { createLogger, format, transports } = require("winston");

const sleep = (delay) => new Promise((resolve) => setTimeout(resolve, delay));

const csvFormat = format.printf(({ timestamp, level, message }) => {
    return `${timestamp},${level.toUpperCase()},${message}`;
  });

const logger = createLogger({
      level:"info",
      format: format.combine(
            format.timestamp({ format: "YYYY-MM-DDTHH:mm:ss.SSS[Z]" }), // ISO 8601 format
            csvFormat
          ),
      transports: [
        new transports.File({ filename: "logs.txt" }),
      ],
    });


//const uri = "mongodb://192.168.17.118:27017"; // Change if necessary
const uri = "mongodb://localhost:27017"; 
const dbName = "services";
const collectionName = "services";
const documentId = "acl"; // Change as needed

async function main() {
    const client = new MongoClient(uri);
    await client.connect();
    const collection = client.db(dbName).collection(collectionName);

    let numTasks = 1;
    while (true) {
        console.log(`Starting ${numTasks} parallel read tasks`);
        const promises = [];

        for (let i = 0; i < numTasks; i++) {
            promises.push((async () => {
                const filter = { id: documentId };
                const retrievalStart = Date.now();

                try {
                    const doc = await collection.findOne(filter);
                    if (doc) {
                        console.log(`Read document: ${JSON.stringify(doc)}`);
                    } else {
                        console.log("Document not found");
                    }
                } catch (e) {
                    console.error(`Error reading document: ${e}`);
                }

                const retrievalEnd = Date.now();
                const retrievalDuration = retrievalEnd - retrievalStart;
                logger.info( `${numTasks}, latency: ${retrievalDuration}ms`);
                console.log(` ${numTasks} parallel reads, latency: ${retrievalDuration}ms`);
            })());
        }

        await Promise.all(promises);
        numTasks += 1; 
        await sleep(2000); // Wait before increasing load
    }
}



main().catch(console.error);