package s3

import (
	"context"
	"io"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type (
	Config struct {
		AccessKeyID     string `json:"access_key_id"`
		SecretAccessKey string `json:"secret_access_key"`
		Region          string `json:"region"`
		Bucket          string `json:"bucket"`
	}
	S3 struct {
		Config   Config
		UploadID string
		Key      string
		Writer   io.WriteCloser
		ETag     chan string
		URL      chan string
		Err      chan error
	}
)

func (s *S3) Session() (newSession *session.Session) {
	newSession = session.New(&aws.Config{
		Credentials: credentials.NewStaticCredentials(s.Config.AccessKeyID, s.Config.SecretAccessKey, ""),
		Region:      aws.String(s.Config.Region),
	})
	return
}

func (s *S3) CreatePartUpload(ctx context.Context) (err chan error) {
	err = make(chan error, 1)
	go func() {
		svc := s3.New(s.Session())
		result, goErr := svc.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(s.Config.Bucket),
			Key:    aws.String(s.Key),
		})
		var resultString string
		if result != nil {
			resultString = awsutil.StringValue(result)
		}
		logrus.Debugf("[S3] CreatePartUpload %s/%s %v %s", s.Config.Bucket, s.Key, goErr, resultString)
		if result != nil && goErr == nil {
			s.UploadID = *result.UploadId
		} else {
			s.UploadID = ""
		}
		err <- goErr
	}()
	return
}

func (s *S3) SetTags(ctx context.Context, tags map[string]string) (err chan error) {
	err = make(chan error, 1)
	go func() {
		tagset := []*s3.Tag{}
		for key, val := range tags {
			tagset = append(tagset, &s3.Tag{
				Key:   aws.String(key),
				Value: aws.String(val),
			})
		}

		svc := s3.New(s.Session())
		result, goErr := svc.PutObjectTaggingWithContext(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(s.Config.Bucket),
			Key:    aws.String(s.Key),
			Tagging: &s3.Tagging{
				TagSet: tagset,
			},
		})
		var resultString string
		if result != nil {
			resultString = awsutil.StringValue(result)
		}
		logrus.Debugf("[S3] SetTags %s/%s %v %s", s.Config.Bucket, s.Key, goErr, resultString)
		err <- goErr
	}()
	return
}

func (s *S3) PartUpload(ctx context.Context, reader io.ReadSeeker, contentLength int, partNumber int) (etag chan string, err chan error) {
	etag = make(chan string, 1)
	err = make(chan error, 1)
	go func() {
		svc := s3.New(s.Session())
		result, goErr := svc.UploadPartWithContext(ctx, &s3.UploadPartInput{
			Body:          reader,
			Bucket:        aws.String(s.Config.Bucket),
			Key:           aws.String(s.Key),
			UploadId:      aws.String(s.UploadID),
			PartNumber:    aws.Int64(int64(partNumber)),
			ContentLength: aws.Int64(int64(contentLength)),
		})

		var resultString string
		if result != nil {
			resultString = awsutil.StringValue(result)
		}
		logrus.Debugf("[S3] PartUpload %s/%s %v %s", s.Config.Bucket, s.Key, goErr, resultString)

		if result != nil && goErr == nil {
			etag <- *result.ETag
		} else {
			etag <- ""
		}
		err <- goErr
	}()
	return
}

func (s *S3) CompletePartUpload(ctx context.Context, etags []string) (url chan string, err chan error) {
	url = make(chan string, 1)
	err = make(chan error, 1)
	go func() {
		svc := s3.New(s.Session())

		parts := []*s3.CompletedPart{}
		for i, etag := range etags {
			parts = append(parts, &s3.CompletedPart{
				PartNumber: aws.Int64(int64(i + 1)),
				ETag:       aws.String(etag),
			})
		}
		result, goErr := svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(s.Config.Bucket),
			Key:      aws.String(s.Key),
			UploadId: aws.String(s.UploadID),
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: parts,
			},
		})

		var resultString string
		if result != nil {
			resultString = awsutil.StringValue(result)
		}
		logrus.Debugf("[S3] CompletePartUpload %s/%s %v %v %s", s.Config.Bucket, s.Key, etags, goErr, resultString)

		if result != nil && goErr == nil {
			url <- *result.Location
		} else {
			url <- ""
		}
		err <- goErr

	}()
	return
}

func (s *S3) Upload(ctx context.Context) (writer *io.PipeWriter, url chan string, err chan error) {
	url = make(chan string, 1)
	err = make(chan error, 1)
	var reader *io.PipeReader
	reader, writer = io.Pipe()
	go func() {
		uploader := s3manager.NewUploader(s.Session(), func(u *s3manager.Uploader) {
			u.Concurrency = 3
		})
		var goErr error
		var result *s3manager.UploadOutput
		result, goErr = uploader.UploadWithContext(ctx, &s3manager.UploadInput{
			Bucket: aws.String(s.Config.Bucket),
			Key:    aws.String(s.Key),
			Body:   reader,
		})

		var resultString string
		if result != nil {
			s.UploadID = result.UploadID
			resultString = awsutil.StringValue(result)
		}
		logrus.Debugf("[S3] Upload %s/%s %v %s", s.Config.Bucket, s.Key, goErr, resultString)

		if result != nil && goErr == nil {
			url <- result.Location
		} else {
			url <- ""
		}
		err <- goErr
	}()
	return
}

func (s *S3) Get(ctx context.Context, start, end int64) (result *s3.GetObjectOutput, reader io.ReadCloser, err error) {
	svc := s3.New(s.Session())
	input := &s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(s.Key),
	}
	if start != 0 || end != 0 {
		val := "bytes=" + strconv.FormatInt(start, 10) + "-"
		if end != 0 {
			val += strconv.FormatInt(end, 10)
		}
		input.Range = aws.String(val)
	}

	result, err = svc.GetObjectWithContext(ctx, input)

	var resultString string
	if result != nil {
		resultString = awsutil.StringValue(result)
	}
	logrus.Debugf("[S3] Get %s/%s %v %s", s.Config.Bucket, s.Key, err, resultString)

	if err != nil {
		return
	}
	reader = result.Body
	return
}

func (s *S3) Copy(ctx context.Context, target string) (err chan error) {
	err = make(chan error, 1)
	go func() {
		time.Sleep(time.Millisecond * 150)
		svc := s3.New(s.Session())
		var result *s3.CopyObjectOutput
		var goErr error
		input := &s3.CopyObjectInput{
			Bucket:     aws.String(s.Config.Bucket),
			CopySource: aws.String(s.Config.Bucket + "/" + s.Key),
			Key:        aws.String(target),
		}
		result, goErr = svc.CopyObjectWithContext(ctx, input)

		var resultString string
		if result != nil {
			resultString = awsutil.StringValue(result)
		}
		logrus.Debugf("[S3] Copy %s/%s -> %s/%s %v %s", s.Config.Bucket, s.Key, s.Config.Bucket, target, goErr, resultString)

		if goErr == nil {
			s.Key = target
		}

		err <- goErr
	}()
	return
}
