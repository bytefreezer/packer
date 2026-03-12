// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/bytefreezer/goodies/log"
)

// S3SourceConfig represents the S3 source configuration
type S3SourceConfig struct {
	BucketName string
	Region     string
	AccessKey  string
	SecretKey  string
	Endpoint   string
	Ssl        bool
	UseIamRole bool
}

type S3SourceClient struct {
	config   S3SourceConfig
	s3Client *s3.Client
	bucket   string
}

type S3Object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

func NewS3SourceClient(s3Config S3SourceConfig) (*S3SourceClient, error) {
	log.Debugf("NewS3SourceClient called with BucketName='%s', Region='%s', Endpoint='%s'",
		s3Config.BucketName, s3Config.Region, s3Config.Endpoint)
	if s3Config.BucketName == "" {
		log.Errorf("S3 source bucket name validation failed - BucketName field is empty")
		return nil, fmt.Errorf("S3 source bucket name is required")
	}

	// Default region to us-east-1 for MinIO/custom endpoints if not specified
	// MinIO doesn't require a specific region, but AWS SDK needs one
	if s3Config.Region == "" {
		s3Config.Region = "us-east-1"
		if s3Config.Endpoint != "" {
			log.Debugf("Defaulting region to us-east-1 for custom endpoint: %s", s3Config.Endpoint)
		}
	}

	// Create AWS config
	var awsCfg aws.Config
	var err error

	// Determine credential source based on UseIamRole flag:
	// - If UseIamRole is true: use default credential chain (IAM role, instance profile, etc.)
	// - If UseIamRole is false: use static credentials (AccessKey and SecretKey)
	if s3Config.UseIamRole {
		// Use default credential chain (IAM role, environment variables, etc.)
		awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(s3Config.Region),
		)
	} else {
		// Use static credentials (access key + secret key)
		awsCfg, err = awsconfig.LoadDefaultConfig(context.Background(),
			awsconfig.WithRegion(s3Config.Region),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				s3Config.AccessKey,
				s3Config.SecretKey,
				"",
			)),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Configure custom endpoint if provided (for MinIO, LocalStack, etc.)
	var s3Client *s3.Client
	if s3Config.Endpoint != "" {
		// Custom endpoint configuration
		endpoint := s3Config.Endpoint
		if !strings.HasPrefix(endpoint, "http") {
			if s3Config.Ssl {
				endpoint = "https://" + endpoint
			} else {
				endpoint = "http://" + endpoint
			}
		}

		s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true                              // Required for MinIO and some custom S3 implementations
			o.DisableS3ExpressSessionAuth = aws.Bool(true)     // Disable S3 Express for MinIO compatibility
			o.DisableLogOutputChecksumValidationSkipped = true // Suppress MinIO checksum warnings
		})
	} else {
		// Standard AWS S3
		s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.DisableLogOutputChecksumValidationSkipped = true // Suppress checksum warnings for AWS S3 too
		})
	}

	client := &S3SourceClient{
		config:   s3Config,
		s3Client: s3Client,
		bucket:   s3Config.BucketName,
	}

	return client, nil
}

// TestConnection tests the S3 source connection
func (sc *S3SourceClient) TestConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Debugf("Testing S3 source connection to bucket: %s", sc.bucket)

	// Use ListObjectsV2 with MaxKeys=1 instead of HeadBucket for better MinIO compatibility
	// HeadBucket can fail with certain S3-compatible storage systems
	_, err := sc.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(sc.bucket),
		MaxKeys: aws.Int32(1),
	})

	if err != nil {
		return fmt.Errorf("failed to access S3 source bucket %s: %w", sc.bucket, err)
	}

	log.Infof("S3 source connection successful to bucket: %s", sc.bucket)
	return nil
}

// ListObjects lists all objects in the S3 source bucket with optional prefix, paginating through all results
func (sc *S3SourceClient) ListObjects(ctx context.Context, prefix string) ([]S3Object, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(sc.bucket),
	}

	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	var objects []S3Object
	for {
		result, err := sc.s3Client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket %s: %w", sc.bucket, err)
		}

		for _, obj := range result.Contents {
			if obj.Key == nil {
				continue
			}

			s3Obj := S3Object{
				Key: *obj.Key,
			}

			if obj.Size != nil {
				s3Obj.Size = *obj.Size
			}

			if obj.LastModified != nil {
				s3Obj.LastModified = *obj.LastModified
			}

			objects = append(objects, s3Obj)
		}

		if !aws.ToBool(result.IsTruncated) || result.NextContinuationToken == nil {
			break
		}
		input.ContinuationToken = result.NextContinuationToken
	}

	log.Debugf("Listed %d objects from S3 source bucket %s with prefix '%s'", len(objects), sc.bucket, prefix)
	return objects, nil
}

// GetObject downloads an object from the S3 source bucket
func (sc *S3SourceClient) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	if key == "" {
		return nil, fmt.Errorf("object key cannot be empty")
	}

	log.Debugf("Getting object %s from S3 source bucket %s", key, sc.bucket)

	result, err := sc.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to get object %s from bucket %s: %w", key, sc.bucket, err)
	}

	return result.Body, nil
}

// DeleteObject deletes an object from the S3 source bucket
func (sc *S3SourceClient) DeleteObject(ctx context.Context, key string) error {
	if key == "" {
		return fmt.Errorf("object key cannot be empty")
	}

	log.Debugf("Deleting object %s from S3 source bucket %s", key, sc.bucket)

	_, err := sc.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		return fmt.Errorf("failed to delete object %s from bucket %s: %w", key, sc.bucket, err)
	}

	log.Debugf("Successfully deleted object %s from S3 source bucket %s", key, sc.bucket)
	return nil
}

// GetBucketInfo returns information about the S3 source bucket
func (sc *S3SourceClient) GetBucketInfo() (string, string, bool) {
	endpoint := sc.config.Endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", sc.config.Region)
	}

	return sc.bucket, endpoint, sc.config.Ssl
}

// ListDataFiles lists .ndjson.gz files in the S3 source bucket
func (sc *S3SourceClient) ListDataFiles(ctx context.Context, prefix string) ([]S3Object, error) {
	objects, err := sc.ListObjects(ctx, prefix)
	if err != nil {
		return nil, err
	}

	// Filter for .ndjson.gz files (both old and new formats)
	dataFiles := make([]S3Object, 0)
	for _, obj := range objects {
		objKey := strings.ToLower(obj.Key)
		// Accept both formats:
		// Old format: batch_123456789.ndjson.gz
		// New format: tenant--dataset--timestamp--ndjson.gz
		if strings.HasSuffix(objKey, ".ndjson.gz") || strings.HasSuffix(objKey, "--ndjson.gz") {
			dataFiles = append(dataFiles, obj)
		}
	}

	log.Debugf("Found %d .ndjson.gz files in S3 source bucket %s", len(dataFiles), sc.bucket)
	return dataFiles, nil
}
