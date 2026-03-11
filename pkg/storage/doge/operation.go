package doge

import (
	"bytes"
	"context"
	"io"

	"github.com/haierkeys/custom-image-gateway/pkg/fileurl"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

func (d *Doge) GetBucket(bucketName string) string {
	if len(bucketName) <= 0 {
		bucketName = d.Config.BucketName
	}
	return bucketName
}

// SendFile 上传文件
func (d *Doge) SendFile(fileKey string, file io.Reader, itype string) (string, error) {
	ctx := context.Background()
	bucket := d.GetBucket("")

	fileKey = fileurl.PathSuffixCheckAdd(d.Config.CustomPath, "/") + fileKey

	client := d.getClient()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(fileKey),
		Body:        file,
		ContentType: aws.String(itype),
	})

	if err != nil {
		return "", errors.Wrap(err, "doge")
	}

	return fileKey, nil
}

// SendContent 上传内容
func (d *Doge) SendContent(fileKey string, content []byte) (string, error) {
	ctx := context.Background()
	bucket := d.GetBucket("")

	fileKey = fileurl.PathSuffixCheckAdd(d.Config.CustomPath, "/") + fileKey

	client := d.getClient()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
		Body:   bytes.NewReader(content),
	})

	if err != nil {
		return "", errors.Wrap(err, "doge")
	}

	return fileKey, nil
}
