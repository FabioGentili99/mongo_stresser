package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var logger = logrus.New()

// Custom CSV Formatter
type CSVFormatter struct{}

func (f *CSVFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// Convert timestamp to ISO 8601 format
	timestamp := entry.Time.UTC().Format("2006-01-02T15:04:05.000Z")
	// Format the log as CSV: timestamp,level,logger,message
	logMsg := fmt.Sprintf("%s,%s,%s\n",
		timestamp, entry.Level.String(), entry.Message)
	return []byte(logMsg), nil
}

func setup() {
	logger.SetFormatter(&logrus.JSONFormatter{TimestampFormat: "2006-01-02T15:04:05.000Z07:00"})
	file, err := os.OpenFile("logs.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err == nil {
		logger.SetOutput(file)
		logger.SetLevel(logrus.InfoLevel)    // Log level
		logger.SetFormatter(&CSVFormatter{}) // Use custom CSV formatter

	} else {
		log.Fatal(err)
	}

}

func main() {
	setup()
	uri := "mongodb://admin:password@192.168.17.118:27017" // Change if necessary
	dbName := "services"
	collectionName := "services"
	documentID := "acl" // Change as needed

	// Initialize MongoDB client
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			log.Fatalf("Failed to disconnect client: %v", err)
		}
	}()

	collection := client.Database(dbName).Collection(collectionName)

	numTasks := 1
	for {
		fmt.Printf("Starting %d parallel read tasks\n", numTasks)
		var handles []chan struct{}

		for i := 0; i < numTasks; i++ {
			handle := make(chan struct{})
			handles = append(handles, handle)

			go func() {

				for j:= 0; j<10; j++{

				filter := bson.D{{Key: "id", Value: documentID}}
				// Start the timer
				retrievalStart := time.Now()

				var result bson.M
				err := collection.FindOne(context.TODO(), filter).Decode(&result)
				if err != nil {
					if err == mongo.ErrNoDocuments {
						fmt.Println("Document not found")
					} else {
						log.Printf("Error reading document: %v", err)
					}
				} else {
					fmt.Printf("Read document: %+v\n", result)
				}

				retrievalDuration := time.Since(retrievalStart)
				logger.Infof("%d parallel reads, latency: %vms", numTasks, retrievalDuration)
				fmt.Printf("%d parallel reads, latency: %v\n", numTasks, retrievalDuration)

				}

				close(handle)
			}()
		}

		for _, handle := range handles {
			<-handle // Wait for each goroutine to finish
		}

		numTasks++ // Double the load each iteration
		if numTasks > 10 {
			break
		}
		time.Sleep(2 * time.Second) // Wait before increasing load
	}
}
