// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Writer implements io.Writer for direct streaming to S3
type S3Writer struct {
	client *s3.Client
	bucket string
	key    string
	buffer *bytes.Buffer
	closed bool
}

// Write implements io.Writer interface
func (w *S3Writer) Write(p []byte) (n int, err error) {
	if w.closed {
		return 0, fmt.Errorf("S3Writer is closed")
	}

	if w.buffer == nil {
		w.buffer = &bytes.Buffer{}
	}

	return w.buffer.Write(p)
}

// Close uploads the buffer to S3 and closes the writer
func (w *S3Writer) Close() error {
	if w.closed {
		return nil
	}

	if w.buffer == nil || w.buffer.Len() == 0 {
		w.closed = true
		return nil
	}

	// Upload to S3
	_, err := w.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
		Body:   bytes.NewReader(w.buffer.Bytes()),
	})

	w.closed = true
	return err
}

// BytesWritten returns the number of bytes written to the buffer
func (w *S3Writer) BytesWritten() int64 {
	if w.buffer == nil {
		return 0
	}
	return int64(w.buffer.Len())
}

// AtomicRename renames the object from temporary key to final key
func (w *S3Writer) AtomicRename(finalKey string) error {
	if !w.closed {
		return fmt.Errorf("writer must be closed before rename")
	}

	// Copy from temp key to final key
	_, err := w.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     aws.String(w.bucket),
		Key:        aws.String(finalKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", w.bucket, w.key)),
	})
	if err != nil {
		return fmt.Errorf("failed to copy object: %w", err)
	}

	// Delete temp key
	_, err = w.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete temp object: %w", err)
	}

	return nil
}
