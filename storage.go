package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Environment string

const (
	Development Environment = "dev"
	Staging     Environment = "stage"
	Production  Environment = "prod"
)

// S3Config holds configuration for S3-compatible storage (AWS S3, Cloudflare R2, MinIO, etc.)
type S3Config struct {
	AccessKeyID     string
	SecretAccessKey string
	BucketName      string
	Region          string
	Endpoint        string
}

type StorageClient struct {
	client      *s3.Client
	bucketName  string
	environment Environment
	devUser     string
}

type FileInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

func NewStorageClient(cfg S3Config, env Environment, devUser string) (*StorageClient, error) {
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID, cfg.SecretAccessKey, "",
		)),
		config.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, err
	}

	var client *s3.Client
	if cfg.Endpoint != "" {
		client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	} else {
		client = s3.NewFromConfig(awsCfg)
	}

	return &StorageClient{
		client:      client,
		bucketName:  cfg.BucketName,
		environment: env,
		devUser:     devUser,
	}, nil
}

// LoadR2ConfigFromEnv loads Cloudflare R2 configuration (legacy, use LoadS3ConfigFromEnv)
func LoadR2ConfigFromEnv() S3Config {
	return S3Config{
		AccessKeyID:     os.Getenv("R2_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("R2_SECRET_ACCESS_KEY"),
		BucketName:      getEnvOrDefault("R2_BUCKET_NAME", "default"),
		Region:          getEnvOrDefault("R2_REGION", "auto"),
		Endpoint:        os.Getenv("R2_ENDPOINT"),
	}
}

func (s *StorageClient) buildPath(path string) string {
	switch s.environment {
	case Development:
		if s.devUser != "" {
			return filepath.Join(string(s.environment), "dev-"+s.devUser, path)
		}
		return filepath.Join(string(s.environment), path)
	default:
		return filepath.Join(string(s.environment), path)
	}
}

func (s *StorageClient) UploadCSV(ctx context.Context, path string, data []byte) error {
	key := s.buildPath("csv/" + path)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("text/csv"),
	})
	return err
}

func (s *StorageClient) DownloadCSV(ctx context.Context, path string) ([]byte, error) {
	key := s.buildPath("csv/" + path)
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()
	return io.ReadAll(result.Body)
}

func (s *StorageClient) ListCSVFiles(ctx context.Context, prefix string) ([]FileInfo, error) {
	key := s.buildPath("csv/" + prefix)
	result, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucketName),
		Prefix: aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	files := make([]FileInfo, 0, len(result.Contents))
	for _, obj := range result.Contents {
		files = append(files, FileInfo{
			Key:          *obj.Key,
			Size:         *obj.Size,
			LastModified: *obj.LastModified,
		})
	}
	return files, nil
}

func (s *StorageClient) UploadScrapedData(ctx context.Context, filename string, data []byte) error {
	path := fmt.Sprintf("scraped/%s/%s", filename)
	return s.UploadCSV(ctx, path, data)
}

func (s *StorageClient) UploadImage(ctx context.Context, path string, data []byte, contentType string) error {
	key := s.buildPath("images/" + path)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucketName),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	return err
}

func (s *StorageClient) TestConnection(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(s.bucketName),
	})
	return err
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
