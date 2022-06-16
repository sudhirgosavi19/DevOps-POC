package main

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	// Initialize a session in ap-south-1 that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials.
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-south-1")},
	)

	// Create S3 service client
	svc := s3.New(sess)

	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		log.Println("Failed to list buckets", err)
		return
	}

	log.Println("Buckets:")

	for _, bucket := range result.Buckets {
		log.Printf("%s : %s\n", aws.StringValue(bucket.Name), bucket.CreationDate)
	}
}
