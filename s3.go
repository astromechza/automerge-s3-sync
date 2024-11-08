package automerge_s3_sync

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
)

type S3 interface {
	GetObject(ctx context.Context, key string, dst io.Writer) (meta map[string]string, err error)
	HeadObject(ctx context.Context, key string) (size int64, meta map[string]string, err error)
	ListObjects(ctx context.Context, prefix string, delimiter string) (keys []string, sizes []int64, prefixes []string, err error)
	PutObject(ctx context.Context, key string, meta map[string]string, body io.Reader) (err error)
	DeleteObject(ctx context.Context, key string) error
}

var ErrObjectNotFound = errors.New("object not found")

type InMemoryS3 struct {
	mux     sync.RWMutex
	objects map[string][]byte
	metas   map[string]map[string]string
}

func (i *InMemoryS3) GetObject(ctx context.Context, key string, dst io.Writer) (meta map[string]string, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	i.mux.RLock()
	defer i.mux.RUnlock()
	meta = maps.Clone(i.metas[key])
	if meta == nil {
		meta = map[string]string{}
	}
	if obj, ok := i.objects[key]; !ok {
		return nil, ErrObjectNotFound
	} else if _, err := dst.Write(obj); err != nil {
		return meta, err
	}
	return meta, nil
}

func (i *InMemoryS3) HeadObject(ctx context.Context, key string) (size int64, meta map[string]string, err error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	i.mux.RLock()
	defer i.mux.RUnlock()
	meta = maps.Clone(i.metas[key])
	if meta == nil {
		meta = map[string]string{}
	}
	if obj, ok := i.objects[key]; !ok {
		return 0, nil, ErrObjectNotFound
	} else {
		return int64(len(obj)), meta, nil
	}
}

type twoSliceSorter struct {
	keySlice  []string
	sizeSlice []int64
}

func (t *twoSliceSorter) Len() int {
	return len(t.keySlice)
}

func (t *twoSliceSorter) Less(i, j int) bool {
	return t.keySlice[i] < t.keySlice[j]
}

func (t *twoSliceSorter) Swap(i, j int) {
	t.keySlice[i], t.keySlice[j] = t.keySlice[j], t.keySlice[i]
	t.sizeSlice[i], t.sizeSlice[j] = t.sizeSlice[j], t.sizeSlice[i]
}

var _ sort.Interface = (*twoSliceSorter)(nil)

func (i *InMemoryS3) ListObjects(ctx context.Context, prefix string, delimiter string) (keys []string, sizes []int64, prefixes []string, err error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	i.mux.RLock()
	defer i.mux.RUnlock()

	keys = make([]string, 0, len(i.objects))
	sizes = make([]int64, 0, len(i.objects))
	prefixSet := make(map[string]bool)

	for key, obj := range i.objects {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if delimiter != "" {
			x := strings.Index(key[len(prefix):], delimiter)
			if x >= 0 {
				prefixSet[key[:len(prefix)+x+len(delimiter)]] = true
				continue
			}
		}
		keys = append(keys, key)
		sizes = append(sizes, int64(len(obj)))
	}

	prefixes = make([]string, 0, len(prefixSet))
	for s := range prefixSet {
		prefixes = append(prefixes, s)
	}
	sort.Strings(prefixes)
	sort.Sort(&twoSliceSorter{keySlice: keys, sizeSlice: sizes})
	return keys, sizes, prefixes, nil
}

func (i *InMemoryS3) PutObject(ctx context.Context, key string, meta map[string]string, body io.Reader) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	i.mux.Lock()
	defer i.mux.Unlock()
	raw, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if i.objects == nil {
		i.objects = make(map[string][]byte)
		i.metas = make(map[string]map[string]string)
	}
	i.objects[key] = bytes.Clone(raw)
	i.metas[key] = maps.Clone(meta)
	return nil
}

func (i *InMemoryS3) DeleteObject(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	i.mux.Lock()
	defer i.mux.Unlock()
	delete(i.objects, key)
	return nil
}

var _ S3 = (*InMemoryS3)(nil)

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
	if !strings.HasSuffix(bucketUrl.Path, "/") {
		bucketUrl.Path += "/"
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
			if resp.StatusCode == http.StatusNotFound {
				return size, meta, ErrObjectNotFound
			}
			bod, _ := io.ReadAll(resp.Body)
			return size, meta, fmt.Errorf("failed to get objects due to status code: %s: %s", resp.Status, string(bod))
		}
		if dst != nil {
			if r.Header.Get("Content-MD5") != "" {
				dst = &hashWriter{H: md5.New(), W: dst}
			}
			if _, err := io.Copy(dst, resp.Body); err != nil {
				return resp.ContentLength, meta, fmt.Errorf("failed to copy response body: %w", err)
			}
			if hA := r.Header.Get("Content-MD5"); hA != "" {
				if hB := base64.StdEncoding.EncodeToString(dst.(*hashWriter).H.Sum(nil)); hA != hB {
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
	return s.readBlob(ctx, key, http.MethodHead, nil)
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
			bod, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("failed to list objects due to status code: %s: %s", resp.Status, string(bod))
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
	sort.Strings(prefixes)
	sort.Sort(&twoSliceSorter{keySlice: keys, sizeSlice: sizes})
	return
}

func (s *S3Impl) PutObject(ctx context.Context, key string, meta map[string]string, body io.Reader) (err error) {
	var checksum string
	if raw, err := io.ReadAll(body); err != nil {
		return fmt.Errorf("failed to read buffered body: %w", err)
	} else {
		h := md5.New()
		_, _ = h.Write(raw)
		checksum = base64.StdEncoding.EncodeToString(h.Sum(nil))
		body = bytes.NewReader(raw)
	}
	if r, err := http.NewRequestWithContext(ctx, http.MethodPut, s.bucketUrl.ResolveReference(&url.URL{Path: key}).String(), body); err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	} else {
		r.Header.Set("Content-MD5", checksum)
		for s2, s3 := range meta {
			r.Header.Set("x-amz-meta-"+s2, s3)
		}
		if resp, err := s.client.Do(r); err != nil {
			return fmt.Errorf("failed to make request: %w", err)
		} else {
			defer func() {
				_ = resp.Body.Close()
			}()
			if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
				raw, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("failed to make put request due to status code: %s: %s", resp.Status, string(raw))
			}
		}
		return nil
	}
}

func (s *S3Impl) DeleteObject(ctx context.Context, key string) error {
	if r, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.bucketUrl.ResolveReference(&url.URL{Path: key}).String(), nil); err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	} else if resp, err := s.client.Do(r); err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	} else {
		defer func() {
			_ = resp.Body.Close()
		}()
		// delete object has various interpretations depending on the storage provider. GCS doesn't support bulk delete and returns
		// a 404 for objects that are not found which is wrong but we should handle it here.
		if (resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices) && resp.StatusCode != http.StatusNotFound {
			bod, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("failed to make delete request due to status code: %s: %s", resp.Status, string(bod))
		}
		return nil
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
		meta = maps.Clone(meta)
		if meta == nil {
			meta = make(map[string]string)
		}
		meta["cipher-mode"] = "GCM"
		nonce := make([]byte, gcm.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			return fmt.Errorf("failed to generate nonce: %w", err)
		}
		return s.S3.PutObject(ctx, key, meta, io.MultiReader(
			bytes.NewReader(nonce),
			bytes.NewReader(gcm.Seal(n[:0], nonce, n, nil)),
		))
	}
}
