package automerge_s3_sync

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
)

type S3 interface {
	GetObject(ctx context.Context, key string, dst io.Writer) (meta map[string]string, err error)
	HeadObject(ctx context.Context, key string) (size int64, meta map[string]string, err error)
	ListObjects(ctx context.Context, prefix string, delimiter string) (keys []string, sizes []int64, prefixes []string, err error)
	PutObject(ctx context.Context, key string, meta map[string]string, body io.Reader) (err error)
	DeleteObjects(ctx context.Context, keys []string) (notDeleted [][2]string, err error)
}

type HttpDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type S3Impl struct {
	client    HttpDoer
	bucketUrl *url.URL
}

func NewS3Impl(client HttpDoer, bucketUrl *url.URL) S3 {
	if client == nil {
		panic("client cannot be nil")
	} else if bucketUrl == nil {
		panic("bucketUrl cannot be nil")
	}
	return &S3Impl{client: client, bucketUrl: bucketUrl}
}

type hashWriter struct {
	H hash.Hash
	W io.Writer
}

func (h *hashWriter) Write(p []byte) (n int, err error) {
	h.H.Write(p)
	return h.W.Write(p)
}

var _ io.Writer = (*hashWriter)(nil)

func (s *S3Impl) readBlob(ctx context.Context, key, method string, dst io.Writer) (size int64, meta map[string]string, err error) {
	if r, err := http.NewRequestWithContext(ctx, method, s.bucketUrl.ResolveReference(&url.URL{Path: key}).String(), nil); err != nil {
		return size, meta, fmt.Errorf("failed to build request: %w", err)
	} else if resp, err := s.client.Do(r); err != nil {
		return size, meta, fmt.Errorf("failed to make request: %w", err)
	} else {
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			return size, meta, fmt.Errorf("failed to make request due to status code: %s", resp.Status)
		}
		if dst != nil {
			if r.Header.Get("Content-MD5") != "" {
				dst = &hashWriter{H: md5.New(), W: dst}
			}
			if _, err := io.Copy(dst, resp.Body); err != nil {
				return resp.ContentLength, meta, fmt.Errorf("failed to copy response body: %w", err)
			}
			if hA := r.Header.Get("Content-MD5"); hA != "" {
				if hB := hex.EncodeToString(dst.(*hashWriter).H.Sum(nil)); hA != hB {
					return resp.ContentLength, meta, fmt.Errorf("integrity check failed: %s != %s", hA, hB)
				}
			}
		}
		outMeta := make(map[string]string)
		for k, v := range resp.Header {
			k = strings.ToLower(k)
			if strings.HasPrefix(k, "x-amz-meta-") {
				outMeta[strings.TrimPrefix(k, "x-amz-meta-")] = v[0]
			}
		}
		return resp.ContentLength, outMeta, nil
	}
}

func (s *S3Impl) GetObject(ctx context.Context, key string, dst io.Writer) (meta map[string]string, err error) {
	_, meta, err = s.readBlob(ctx, key, http.MethodGet, dst)
	return meta, err
}

func (s *S3Impl) HeadObject(ctx context.Context, key string) (size int64, meta map[string]string, err error) {
	return s.readBlob(ctx, http.MethodHead, key, nil)
}

// listObjectsV2 performs https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html.
func (s *S3Impl) listObjectsV2(ctx context.Context, prefix, delimiter, continuationToken string) (*ListBucketResult, error) {
	q := make(url.Values)
	q.Set("list-type", "2")
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	if delimiter != "" {
		q.Set("delimiter", delimiter)
	}
	if continuationToken != "" {
		q.Set("continuation-token", continuationToken)
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, s.bucketUrl.ResolveReference(&url.URL{
		RawQuery: q.Encode(),
	}).String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build list objects request: %w", err)
	}
	resp, err := s.client.Do(r)
	if err != nil {
		return nil, fmt.Errorf("failed to make list objects request: %w", err)
	} else {
		defer func() {
			_ = resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("failed to list objects: %s", resp.Status)
		}
		var out ListBucketResult
		if err := xml.NewDecoder(resp.Body).Decode(&out); err != nil {
			return nil, fmt.Errorf("failed to decode list objects response: %w", err)
		}
		return &out, nil
	}
}

func expand[k any](in []k, n int) []k {
	out := make([]k, len(in)+n)
	copy(out, in)
	return out[:len(in)]
}

func (s *S3Impl) ListObjects(ctx context.Context, prefix string, delimiter string) (keys []string, sizes []int64, prefixes []string, err error) {
	keys, sizes, prefixes = make([]string, 0), make([]int64, 0), make([]string, 0)
	continuationToken := ""
	for {
		r, err := s.listObjectsV2(ctx, prefix, delimiter, continuationToken)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to list objects: %w", err)
		}
		keys = expand(keys, len(r.Contents))
		sizes = expand(sizes, len(r.Contents))
		for _, content := range r.Contents {
			keys = append(keys, content.Key)
			sizes = append(sizes, content.Size)
		}
		prefixes = expand(prefixes, len(r.CommonPrefixes))
		for _, prefix := range r.CommonPrefixes {
			prefixes = append(prefixes, prefix.Prefix)
		}
		if r.IsTruncated {
			continuationToken = r.NextContinuationToken
		} else {
			break
		}
	}
	return
}

func (s *S3Impl) PutObject(ctx context.Context, key string, meta map[string]string, body io.Reader) (err error) {
	var md5h string
	if raw, err := io.ReadAll(body); err != nil {
		return fmt.Errorf("failed to read buffered body: %w", err)
	} else {
		h := md5.New()
		_, _ = h.Write(raw)
		md5h = hex.EncodeToString(h.Sum(nil))
		body = bytes.NewReader(raw)
	}
	if r, err := http.NewRequestWithContext(ctx, http.MethodPut, s.bucketUrl.ResolveReference(&url.URL{Path: key}).String(), body); err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	} else {
		r.Header.Set("Content-MD5", md5h)
		for s2, s3 := range meta {
			r.Header.Set("x-amz-meta-"+s2, s3)
		}
		if resp, err := s.client.Do(r); err != nil {
			return fmt.Errorf("failed to make request: %w", err)
		} else {
			defer func() {
				_ = resp.Body.Close()
			}()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("failed to make request due to status code: %s", resp.Status)
			}
		}
		return nil
	}
}

type deleteObjectsBody struct {
	XMLName xml.Name              `xml:"Delete"`
	Objects []deleteObjectsObject `xml:"Object"`
	Quiet   bool                  `xml:"Quiet"`
}

type deleteObjectsObject struct {
	Key string `xml:"Key"`
}

type errorResp struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	RequestId string   `xml:"RequestId"`
}

type deleteObjectsResp struct {
	XMLName xml.Name                 `xml:"DeleteResult"`
	Errors  []deleteObjectsRespError `xml:"Error"`
}

type deleteObjectsRespError struct {
	XMLName xml.Name `xml:"Error"`
	Key     string   `xml:"Key"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func (s *S3Impl) DeleteObjects(ctx context.Context, keys []string) (notDeleted [][2]string, err error) {
	if len(keys) == 0 {
		return nil, nil
	} else if len(keys) == 1 {
		if r, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.bucketUrl.ResolveReference(&url.URL{Path: keys[0]}).String(), nil); err != nil {
			return nil, fmt.Errorf("failed to build request: %w", err)
		} else if resp, err := s.client.Do(r); err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		} else {
			defer func() {
				_ = resp.Body.Close()
			}()
			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("failed to make request due to status code: %s", resp.Status)
			}
		}
		return nil, nil
	} else if len(keys) >= 1000 {
		return nil, fmt.Errorf("delete not implemented for >= 1000 items")
	} else {
		body := &deleteObjectsBody{Objects: make([]deleteObjectsObject, 0, len(keys)), Quiet: true}
		for _, k := range keys {
			body.Objects = append(body.Objects, deleteObjectsObject{Key: k})
		}
		rawBod, _ := xml.Marshal(body)
		if r, err := http.NewRequestWithContext(ctx, http.MethodPost, s.bucketUrl.ResolveReference(&url.URL{RawQuery: "delete"}).String(), bytes.NewReader(rawBod)); err != nil {
			return nil, fmt.Errorf("failed to build request: %w", err)
		} else if resp, err := s.client.Do(r); err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		} else {
			defer func() {
				_ = resp.Body.Close()
			}()
			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("failed to make request due to status code: %s", resp.Status)
			}
			buff, _ := io.ReadAll(resp.Body)
			var e errorResp
			if _ = xml.Unmarshal(buff, &e); e.Code != "" {
				return nil, fmt.Errorf("delete request returned an error: %s: %s (%s)", e.Code, e.Message, e.RequestId)
			}
			var de deleteObjectsResp
			if err := xml.Unmarshal(buff, &de); err != nil {
				return nil, fmt.Errorf("failed to unmarshal delete response: %w", err)
			}
			notDeleted = make([][2]string, len(de.Errors))
			for i, respError := range de.Errors {
				notDeleted[i] = [2]string{respError.Key, respError.Code}
			}
			return notDeleted, nil
		}
	}
}

type ListBucketResult struct {
	IsTruncated           bool                     `xml:"IsTruncated"`
	NextContinuationToken string                   `xml:"NextContinuationToken"`
	Contents              []ListBucketObject       `xml:"Contents"`
	CommonPrefixes        []ListBucketCommonPrefix `xml:"CommonPrefixes"`
}

type ListBucketCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type ListBucketObject struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
}

var _ S3 = (*S3Impl)(nil)

type ClientEncryptedS3 struct {
	S3
	BlockCipher cipher.Block
}

func (s *ClientEncryptedS3) GetObject(ctx context.Context, key string, dst io.Writer) (meta map[string]string, err error) {
	buff := new(bytes.Buffer)
	if meta, err = s.S3.GetObject(ctx, key, buff); err != nil {
		return nil, err
	} else if metaCipherMode := meta["cipher-mode"]; metaCipherMode != "GCM" {
		return nil, fmt.Errorf("object meta cipher-mode '%s' != GCM", metaCipherMode)
	} else if gcm, err := cipher.NewGCM(s.BlockCipher); err != nil {
		return nil, fmt.Errorf("failed to initialise gcm cipher: %w", err)
	} else if buff.Len() < gcm.NonceSize() {
		return nil, fmt.Errorf("data size is too small to read gcm nonce")
	} else {
		n := make([]byte, gcm.NonceSize())
		_, _ = buff.Read(n)
		b := buff.Bytes()
		if bo, err := gcm.Open(b[:0], n, b, nil); err != nil {
			return nil, fmt.Errorf("failed to decrypt: %w", err)
		} else if _, err = dst.Write(bo); err != nil {
			return nil, fmt.Errorf("failed to write: %w", err)
		}
		return meta, nil
	}
}

func (s *ClientEncryptedS3) PutObject(ctx context.Context, key string, meta map[string]string, body io.Reader) (err error) {
	if gcm, err := cipher.NewGCM(s.BlockCipher); err != nil {
		return fmt.Errorf("failed to initialise gcm cipher: %w", err)
	} else if n, err := io.ReadAll(body); err != nil {
		return fmt.Errorf("failed to buffer data: %w", err)
	} else {
		nonce := make([]byte, gcm.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return fmt.Errorf("failed to generate nonce: %w", err)
		}
		meta = maps.Clone(meta)
		meta["cipher-mode"] = "GCM"
		return s.S3.PutObject(ctx, key, meta, bytes.NewReader(gcm.Seal(n[:0], nonce, n, nil)))
	}
}