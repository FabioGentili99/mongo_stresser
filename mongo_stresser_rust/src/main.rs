use log::info;
use mongodb::options::FindOneOptions;
use mongodb::{bson::doc, options::ClientOptions, Client};
use std::process::exit;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::task;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    log4rs::init_file("log4rs.yml", Default::default()).unwrap();

    let uri = "mongodb://admin:password@192.168.17.118:27017"; // Change if necessary
    //let uri = "mongodb://localhost:27017";
    let db_name = "services";
    let collection_name = "services";
    let document_id = "acl"; // Change as needed

    // Initialize MongoDB client
    let mut client_options = ClientOptions::parse(uri).await.expect("Failed to parse options");
    client_options.direct_connection = Some(true);
    //client_options.max_pool_size = Some(10);
    let client = Client::with_options(client_options).expect("Failed to create client");
    let collection = client.database(db_name).collection::<mongodb::bson::Document>(collection_name);
    let collection = Arc::new(collection);


    let mut num_tasks = 1;
    loop {
        println!("Starting {} parallel read tasks", num_tasks);
        let mut handles = vec![];
        
        for _ in 0..num_tasks {
            let collection_clone = Arc::clone(&collection);
            let find_options = FindOneOptions::builder()
                .sort(doc! { "$natural": -1 }) // Helps keep document in cache
                .build();
            let handle = task::spawn(async move {

		for _ in 0..100{
                let filter = doc! { "id": document_id };
                // Start the timer
                let retrieval_start = SystemTime::now();

                match collection_clone.find_one(filter).with_options(find_options.clone()).await {
                    Ok(Some(doc)) => println!("Read document: {:?}", doc),
                    Ok(None) => println!("Document not found"),
                    Err(e) => eprintln!("Error reading document: {}", e),
                }

                let retrieval_end = SystemTime::now();
                let retrieval_duration = retrieval_end
                    .duration_since(retrieval_start)
                    .expect("Clock may have gone backwards");
                info!(" {:?} parallel reads, latency: {:?}", num_tasks, retrieval_duration);
                println!(" {:?} parallel reads, latency: {:?}", num_tasks, retrieval_duration)

            	}
		});
            handles.push(handle);
        }

        for handle in handles {
            handle.await.expect("Task failed");
        }

        num_tasks += 1; // Double the load each iteration
        if num_tasks > 10 {
            exit(0);
        }
        sleep(Duration::from_secs(2)).await; // Wait before increasing load
    }
}
