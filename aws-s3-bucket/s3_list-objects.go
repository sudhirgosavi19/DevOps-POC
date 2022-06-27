package main

import (
	"bufio"
	"compress/gzip"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/dezerv/kit/dto"

	"github.com/dezerv/kit/email"
	dezervaws "github.com/dezerv/kit/rpc/aws"
)

// Lists the items in the specified S3 Bucket
//
// Usage:
//    go run s3_list_objects.go BUCKET_NAME
func sum(array []float64) float64 {
	result := 0.0
	for _, v := range array {
		result += v
	}
	return result
}

func main() {
	currentTime := time.Now()
	fmt.Println("YYYY-MM-DD : ", currentTime.Format("2006-01-02"))

	if len(os.Args) != 2 {
		exitErrorf("Bucket name required\nUsage: %s bucket_name",
			os.Args[0])
	}

	// credentials from the shared credentials file ~/.aws/credentials.
	bucket := os.Args[1]

	// Initialize a session in us-west-2 that the SDK will use to load
	sess, err1 := session.NewSession(&aws.Config{
		Region: aws.String("ap-south-1")},
	)
	if err1 != nil {
		exitErrorf("Unable to access AWS account %v", err1)
	}
	svc := s3.New(sess)
	s3Service := dezervaws.NewS3()
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", bucket, err)
	}
	var attachmentErr error = nil
	ad := 0
	attachments := []dto.EmailAttachment{}
	var d_costArr []float64
	var m_costArr []float64
	for _, item := range resp.Contents {
		//fmt.Println("Name:         ", *item.Key)
		modDate := *item.LastModified
		if currentTime.Format("2006-01-02") != modDate.Format("2006-01-02") {
			continue
		}
		if filepath.Ext(*item.Key) != ".gz" {
			continue
		}
		fmt.Println("file name : ", *item.Key)
		ad++
		var tempFilePath *os.File

		fname := "file-" + fmt.Sprint(ad) + "-" + currentTime.Format("2006-01-02") + ".csv.gz"
		tempFilePath, _ = ioutil.TempFile("", fname)
		defer os.Remove(tempFilePath.Name())
		err = s3Service.DownloadFileWithBucketParameter(*item.Key, tempFilePath.Name(), bucket)
		if err != nil {
			attachmentErr = fmt.Errorf("error reading file from s3::%s", err)
			return
		}
		gzFile, err := os.Open(tempFilePath.Name())
		if err != nil {
			attachmentErr = fmt.Errorf("error opening pdf file %s", err)
			return
		}
		defer gzFile.Close()
		reader := bufio.NewReader(gzFile)
		b, err := ioutil.ReadAll(reader)
		if err != nil {
			attachmentErr = fmt.Errorf("error reading the file %s", err)
			return
		}

		content := base64.StdEncoding.EncodeToString(b)

		fmt.Println("File name : ", fname)
		f, err := os.Open(tempFilePath.Name())
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		gr, err := gzip.NewReader(f)
		if err != nil {
			log.Fatal(err)
		}
		defer gr.Close()
		cr := csv.NewReader(gr)
		rec, err := cr.ReadAll()
		if err != nil {
			log.Fatal(err)
		}
		for i, record := range rec {
			if i == 0 {
				continue
			}
			matched, err := regexp.MatchString(currentTime.Format("2006-01-02"), record[1])

			if matched {
				if record[9] != "Credit" {
					//fmt.Println(record[9])
					rec, err := strconv.ParseFloat(record[23], 32)
					d_costArr = append(d_costArr, rec)
					if err != nil {
						fmt.Println("There is an error converting string to float.")
					}
				} else {
					continue
				}
			} else if err != nil {
				continue
			}
			if record[9] != "Credit" {
				//fmt.Println(record[9])
				rec1, err := strconv.ParseFloat(record[23], 32)
				m_costArr = append(m_costArr, rec1)
				if err != nil {
					fmt.Println("There is an error converting string to float.")
				}
			} else {
				continue
			}

		}

		attachment1 := dto.EmailAttachment{
			Content:     content,
			Type:        "application/gzip",
			Name:        fname,
			Disposition: "attachment",
		}
		attachments = append(attachments, attachment1)
	}

	d_cost := sum(d_costArr)
	m_cost := sum(m_costArr)
	s1 := fmt.Sprintf("%.2f", d_cost)
	s2 := fmt.Sprintf("%.2f", m_cost)

	econtent := "Hi All," + "\n" + "The AWS usage cost of the date: " + currentTime.Format("2006-01-02") + " is USD " + s1 + " and for the month, till date is USD " + s2

	fmt.Println(attachmentErr)

	emailReportFromEmail := "sudhir.gosavi@dezerv.in"
	emailReportFromName := "Sudhir"
	//emailReportFromEmail := os.Getenv("EMAIL_REPORT_FROM_EMAIL")
	//emailReportFromName := os.Getenv("EMAIL_REPORT_FROM_NAME")
	emailReportFromSubject := "AWS dashboard billing report" //os.Getenv("EMAIL_REPORT_FROM_SUBJECT")

	from := dto.EmailAddress{Name: emailReportFromName, Email: emailReportFromEmail}
	tos := []dto.EmailAddress{{Email: "sudhir.gosavi@dezerv.in"}, {Email: "yashu.gupta@dezerv.in"}}
	cc := []dto.EmailAddress{{Email: "ankit@dezerv.in"}}
	aclClient := email.NewAclClient("SjlHN1R3cXVaYmJuTy9FMElnL0xJSno0UXM4PQ")
	emailContent := econtent
	err = email.SendWithCC(aclClient, from, tos, nil, cc, &emailReportFromSubject, &emailContent, nil, attachments)

	if err != nil {
		exitErrorf("error sending email using acl: %s", err)
	}

}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}
