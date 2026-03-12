// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/bytefreezer/goodies/log"
)

// S3Config represents the S3 configuration for both source and destination
type S3Config struct {
	BucketName string
	Region     string
	AccessKey  string
	SecretKey  string
	Endpoint   string
	Ssl        bool
	UseIamRole bool
}

type S3Client struct {
	config   S3Config
	s3Client *s3.Client
	bucket   string
}

type S3ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

func NewS3Client(s3Config S3Config) (*S3Client, error) {
	if s3Config.BucketName == "" {
		return nil, fmt.Errorf("S3 bucket name is required")
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

	// Create S3 client with custom endpoint if provided (for MinIO)
	var s3Client *s3.Client
	if s3Config.Endpoint != "" {
		// Custom endpoint configuration (MinIO, LocalStack, etc.)
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
			o.DisableLogOutputChecksumValidationSkipped = true // Suppress MinIO checksum warnings
		})
	} else {
		// Standard AWS S3
		s3Client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.DisableLogOutputChecksumValidationSkipped = true // Suppress checksum warnings for AWS S3 too
		})
	}

	return &S3Client{
		config:   s3Config,
		s3Client: s3Client,
		bucket:   s3Config.BucketName,
	}, nil
}

// TestConnection verifies that the S3 client can connect to the bucket
func (sc *S3Client) TestConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to head the bucket to verify connectivity
	_, err := sc.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(sc.bucket),
	})
	if err != nil {
		return fmt.Errorf("failed to connect to S3 bucket %s: %w", sc.bucket, err)
	}

	log.Infof("Successfully connected to S3 bucket: %s", sc.bucket)
	return nil
}

// GetBucketInfo returns bucket configuration details for logging
func (sc *S3Client) GetBucketInfo() (string, string, bool) {
	endpoint := sc.config.Endpoint
	if endpoint == "" {
		endpoint = "s3.amazonaws.com"
	}
	return sc.bucket, endpoint, sc.config.Ssl
}

// ListObjects lists all objects in the bucket with the given prefix, paginating through all results
func (sc *S3Client) ListObjects(ctx context.Context, prefix string) ([]*S3ObjectInfo, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(sc.bucket),
	}

	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}

	var objects []*S3ObjectInfo
	for {
		resp, err := sc.s3Client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range resp.Contents {
			objects = append(objects, &S3ObjectInfo{
				Key:          aws.ToString(obj.Key),
				Size:         aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
			})
		}

		if !aws.ToBool(resp.IsTruncated) || resp.NextContinuationToken == nil {
			break
		}
		input.ContinuationToken = resp.NextContinuationToken
	}

	return objects, nil
}

// GetObject downloads an object from S3
func (sc *S3Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := sc.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", key, err)
	}

	return resp.Body, nil
}

// PutObject uploads an object to S3
func (sc *S3Client) PutObject(ctx context.Context, key string, body io.Reader, metadata map[string]string) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(key),
		Body:   body,
	}

	if metadata != nil {
		input.Metadata = metadata
	}

	_, err := sc.s3Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to put object %s: %w", key, err)
	}

	return nil
}

// DeleteObject deletes an object from S3
func (sc *S3Client) DeleteObject(ctx context.Context, key string) error {
	_, err := sc.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object %s: %w", key, err)
	}

	return nil
}

// PutObjectBytes uploads bytes directly to S3
func (sc *S3Client) PutObjectBytes(ctx context.Context, key string, data []byte, metadata map[string]string) error {
	return sc.PutObject(ctx, key, bytes.NewReader(data), metadata)
}

// ObjectExists checks if an object exists in S3
func (sc *S3Client) ObjectExists(ctx context.Context, bucketName, key string) (bool, error) {
	bucket := bucketName
	if bucket == "" {
		bucket = sc.bucket
	}

	_, err := sc.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nf *types.NotFound
		if errors.As(err, &nf) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check object existence %s: %w", key, err)
	}

	return true, nil
}

// GetObjectInfo retrieves metadata information about an S3 object
func (sc *S3Client) GetObjectInfo(ctx context.Context, key string) (*S3ObjectInfo, error) {
	resp, err := sc.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(sc.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object info for %s: %w", key, err)
	}

	info := &S3ObjectInfo{
		Key:  key,
		Size: *resp.ContentLength,
	}

	if resp.LastModified != nil {
		info.LastModified = *resp.LastModified
	}

	return info, nil
}

// GetObjectReader returns a reader for the S3 object
func (sc *S3Client) GetObjectReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return sc.GetObject(ctx, key)
}

// GetRawS3Client returns the underlying AWS S3 client for direct access
func (sc *S3Client) GetRawS3Client() *s3.Client {
	return sc.s3Client
}
