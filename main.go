package main

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"log"
	"path/filepath"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

var invokeCount = 0

type MyEvent struct {
	OriginBucket    string `json:"originBucket"`
	ArchiveFilePath string `json:"filePath"`
	TargetBucket    string `json:"targetBucket"`
	TargetFilePath  string `json:"targetFilePath"`
}

type UnzipperResponse struct {
	Success  bool   `json:"Success"`
	Message  string `json:"Message"`
	S3Bucket string `json:"S3Bucket"`
	S3Prefix string `json:"S3Key"`
	Count    int    `json:"Count"`
}

var s3Client *s3.Client

func init() {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("ca-central-1"),
	)
	if err != nil {
		log.Fatal(err)
	}
	s3Client = s3.NewFromConfig(cfg)
}

func upload(bucket, prefix string, file *zip.File) {
	destination := filepath.Join(prefix, file.Name)
	log.Println("Uploading", destination)

	zippedFile, err := file.Open()
	defer zippedFile.Close()

	if err != nil {
		log.Println(err, "Failed to read file", file.Name)
		return
	}

	buff := bytes.NewBuffer([]byte{})
	defer buff.Reset()
	_, err = io.Copy(buff, zippedFile)
	if err != nil {
		log.Println(err, "Failed to read file", file.Name)
		return
	}
	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(destination),
		Body:   buff,
	})

	if err != nil {
		log.Println(err, "Failed to upload file", file.Name)
	} else {
		log.Println("Uploaded", destination, file.Name)
	}

}

func extract(zipFile *zip.Reader, bucket, target string) error {
	log.Println("Starting to extract file")
	var wg sync.WaitGroup
	wg.Add(len(zipFile.File))
	for _, f := range zipFile.File {
		go func(file *zip.File) {
			defer wg.Done()
			upload(bucket, target, file)
		}(f)
	}
	wg.Wait()
	log.Println("Finished extracting file")
	return nil
}

func download(bucket, key string) (*zip.Reader, error) {
	log.Println("Starting to download file")

	result, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	defer result.Body.Close()

	if err != nil {
		return nil, err
	}

	log.Println("Finished downloading file")
	buff := bytes.NewBuffer([]byte{})
	defer buff.Reset()
	size, err := io.Copy(buff, result.Body)
	if err != nil {
		return nil, err
	}

	return zip.NewReader(bytes.NewReader(buff.Bytes()), size)
}

func HandleRequest(ctx context.Context, event MyEvent) (UnzipperResponse, error) {
	invokeCount++
	log.Println("Invoked times:", invokeCount)
	reader, err := download(event.OriginBucket, event.ArchiveFilePath)
	if err != nil {
		log.Fatal(err)
	}
	err = extract(reader, event.TargetBucket, event.TargetFilePath)
	if err != nil {
		log.Fatal(err)
	}
	return UnzipperResponse{
		Message:  "Success",
		S3Bucket: event.TargetBucket,
		S3Prefix: event.TargetFilePath,
		Count:    len(reader.File),
		Success:  true}, nil
}

func main() {
	lambda.Start(HandleRequest)
}
