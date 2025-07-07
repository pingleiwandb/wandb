package filetransfer

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/wandb/wandb/core/internal/observability"
	"github.com/wandb/wandb/core/internal/wboperation"
)

// DefaultFileTransfer uploads or downloads files to/from the server
type DefaultFileTransfer struct {
	// client is the HTTP client for the file transfer
	client *retryablehttp.Client
	// TODO: Remove it after benchmarking.
	hm *httpRequestMaker

	// logger is the logger for the file transfer
	logger *observability.CoreLogger

	// fileTransferStats is used to track upload/download progress
	fileTransferStats FileTransferStats
}

type httpRequestMaker struct {
	client *http.Client

	mu  sync.Mutex
	ips map[string]time.Time
}

// NewDefaultFileTransfer creates a new fileTransfer
func NewDefaultFileTransfer(
	client *retryablehttp.Client,
	logger *observability.CoreLogger,
	fileTransferStats FileTransferStats,
) *DefaultFileTransfer {
	maker := &httpRequestMaker{
		ips: make(map[string]time.Time),
	}
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			// Track ips to know how many hosts we are connecting to
			if tcpConn, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
				maker.mu.Lock()
				ip := tcpConn.IP.String()
				totalHosts := len(maker.ips)
				if _, ok := maker.ips[ip]; ok {
					logger.Info("file transfer: upload: already connected to ip", "ip", ip, "totalHosts", totalHosts)
				} else {
					maker.ips[ip] = time.Now()
					logger.Info("file transfer: upload: conneting to new ip", "ip", ip, "totalHosts", totalHosts)
				}
				maker.mu.Unlock()
			}
			return conn, nil
		},
		// TODO: Why we are still using HTTP/1.1 base on response header ...
		// ForceAttemptHTTP2: true,

		// TODO: Disable keep alive allow us to connect to more hosts and improve throughput.
		// But i only worked on GCP, on AWS it is getting worse.
		// We can consider copy s3's dns logic https://github.com/aws/aws-sdk-go-v2/blob/main/feature/s3/transfermanager/rrdns.go
		// This allows reusing connection while connecting to more hosts.
		DisableKeepAlives:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	maker.client = &http.Client{
		Transport: tr,
	}
	fileTransfer := &DefaultFileTransfer{
		logger:            logger,
		client:            client,
		hm:                maker,
		fileTransferStats: fileTransferStats,
	}
	return fileTransfer
}

// Upload uploads a file to the server
func (ft *DefaultFileTransfer) Upload(task *DefaultUploadTask) error {
	ft.logger.Debug("default file transfer: uploading file", "path", task.Path, "url", task.Url)

	start := time.Now()
	waitForStart := time.Duration(0)
	if !task.CreatedAt.IsZero() {
		waitForStart = time.Since(task.CreatedAt)
	}

	// open the file for reading and defer closing it
	file, err := os.Open(task.Path)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			ft.logger.CaptureError(
				fmt.Errorf(
					"file transfer: upload: error closing file %s: %v",
					task.Path,
					err,
				))
		}
	}(file)

	requestBody, err := getUploadRequestBody(task, file, ft.fileTransferStats, ft.logger)
	if err != nil {
		return err
	}

	req, err := retryablehttp.NewRequest(http.MethodPut, task.Url, requestBody)
	if err != nil {
		return err
	}
	for _, header := range task.Headers {
		parts := strings.SplitN(header, ":", 2)
		if len(parts) != 2 {
			ft.logger.Error("file transfer: upload: invalid header", "header", header)
			continue
		}
		req.Header.Set(parts[0], parts[1])
	}
	if task.Context != nil {
		req = req.WithContext(task.Context)
	}
	resp, err := ft.hm.client.Do(req.Request)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("file transfer: upload: failed to upload: %s", resp.Status)
	}
	task.Response = resp

	spent := time.Since(start)
	if task.ChunkSize > 0 {
		spentMs := spent.Milliseconds()
		waitForStartMs := waitForStart.Milliseconds()
		speedMBPerSec := float64(task.ChunkSize) / 1024 / 1024 / spent.Seconds()
		ft.logger.Info("file transfer: uploaded chunk",
			"chunk", int(task.Offset)/task.ChunkSize,
			"speed", speedMBPerSec,
			"spentMs", spentMs,
			"waitForStartMs", waitForStartMs,
			"offset", task.Offset,
			"chunkSize", task.ChunkSize,
		)
	}
	return nil
}

// Download downloads a file from the server
func (ft *DefaultFileTransfer) Download(task *DefaultDownloadTask) error {
	ft.logger.Debug("default file transfer: downloading file", "path", task.Path, "url", task.Url)
	dir := path.Dir(task.Path)

	// Check if the directory already exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// Directory doesn't exist, create it
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			// Handle the error if it occurs
			return err
		}
	} else if err != nil {
		// Handle other errors that may occur while checking directory existence
		return err
	}

	// TODO: redo it to use the progress writer, to track the download progress
	resp, err := ft.client.Get(task.Url)
	if err != nil {
		return err
	}
	task.Response = resp

	// open the file for writing and defer closing it
	file, err := os.Create(task.Path)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			ft.logger.CaptureError(
				fmt.Errorf(
					"file transfer: download: error closing file %s: %v",
					task.Path,
					err,
				))
		}
	}(file)

	defer func(file io.ReadCloser) {
		if err := file.Close(); err != nil {
			ft.logger.CaptureError(
				fmt.Errorf(
					"file transfer: download: error closing response reader: %v",
					err,
				))
		}
	}(resp.Body)

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

func getUploadRequestBody(
	task *DefaultUploadTask,
	file *os.File,
	fileTransferStats FileTransferStats,
	logger *observability.CoreLogger,
) (io.Reader, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf(
			"file transfer: upload: error when stat-ing %s: %v",
			task.Path,
			err,
		)
	}

	// Don't try to upload directories.
	if stat.IsDir() {
		return nil, fmt.Errorf(
			"file transfer: upload: cannot upload directory %v",
			task.Path,
		)
	}

	if task.Offset+task.Size > stat.Size() {
		// If the range exceeds the file size, there was some kind of error upstream.
		return nil, fmt.Errorf("file transfer: upload: offset + size exceeds the file size")
	}

	if task.Size == 0 {
		// If Size is 0, upload the remainder of the file.
		task.Size = stat.Size() - task.Offset
	}

	// Due to historical mistakes, net/http interprets a 0 value of
	// Request.ContentLength as "unknown" if the body is non-nil, and
	// doesn't send the Content-Length header which is usually required.
	//
	// To have it understand 0 as 0, the body must be set to nil or
	// the NoBody sentinel.
	var requestBody io.Reader
	if task.Size == 0 {
		requestBody = http.NoBody
	} else {
		if task.Size > math.MaxInt {
			return nil, fmt.Errorf("file transfer: file too large (%d bytes)", task.Size)
		}

		progress, err := wboperation.Get(task.Context).NewProgress()
		if err != nil {
			logger.CaptureError(fmt.Errorf("file transfer: %v", err))
		}

		requestBody = NewProgressReader(
			io.NewSectionReader(file, task.Offset, task.Size),
			int(task.Size),
			func(processed int, total int) {
				if task.ProgressCallback != nil {
					task.ProgressCallback(processed, total)
				}

				progress.SetBytesOfTotal(processed, total)

				fileTransferStats.UpdateUploadStats(FileUploadInfo{
					FileKind:      task.FileKind,
					Path:          task.Path,
					UploadedBytes: int64(processed),
					TotalBytes:    int64(total),
				})
			},
		)
	}
	return requestBody, nil
}

type ProgressReader struct {
	io.ReadSeeker
	len      int
	read     int
	callback func(processed, total int)
}

func NewProgressReader(
	reader io.ReadSeeker,
	size int,
	callback func(processed, total int),
) *ProgressReader {
	return &ProgressReader{
		ReadSeeker: reader,
		len:        size,
		callback:   callback,
	}
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.ReadSeeker.Read(p)
	if err != nil {
		return n, err // Return early if there's an error
	}

	pr.read += n
	if pr.callback != nil {
		pr.callback(pr.read, pr.len)
	}
	return n, err
}

func (pr *ProgressReader) Len() int {
	return int(pr.len)
}
