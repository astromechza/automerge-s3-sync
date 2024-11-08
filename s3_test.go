package automerge_s3_sync

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/rand"
	"io"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"
)

func testS3Interface(t *testing.T, impl S3) {

	cleanup := func(t *testing.T) {
		// cleanup bucket
		k, _, _, err := impl.ListObjects(context.Background(), "", "")
		AssertEqual(t, err, nil)
		nd, err := impl.DeleteObjects(context.Background(), k)
		AssertEqual(t, err, nil)
		AssertEqual(t, len(nd), 0)
	}

	defer cleanup(t)

	t.Run("pre-test cleanup", cleanup)

	var lO int64
	if _, ok := impl.(*ClientEncryptedS3); ok {
		lO = 28
	}

	t.Run("empty state", func(t *testing.T) {
		t.Run("list", func(t *testing.T) {
			k, s, p, err := impl.ListObjects(context.Background(), "", "")
			AssertEqual(t, err, nil)
			AssertEqual(t, len(k), 0)
			AssertEqual(t, len(s), 0)
			AssertEqual(t, len(p), 0)

			k, s, p, err = impl.ListObjects(context.Background(), "thing/", "/")
			AssertEqual(t, err, nil)
			AssertEqual(t, len(k), 0)
			AssertEqual(t, len(s), 0)
			AssertEqual(t, len(p), 0)
		})
		t.Run("head", func(t *testing.T) {
			n, m, err := impl.HeadObject(context.Background(), "thing")
			AssertErrorIs(t, err, ErrObjectNotFound)
			AssertEqual(t, m, nil)
			AssertEqual(t, n, 0)
		})
		t.Run("get", func(t *testing.T) {
			m, err := impl.GetObject(context.Background(), "thing", io.Discard)
			AssertErrorIs(t, err, ErrObjectNotFound)
			AssertEqual(t, m, nil)
		})
		t.Run("delete", func(t *testing.T) {
			nd, err := impl.DeleteObjects(context.Background(), []string{"thing"})
			AssertEqual(t, err, nil)
			AssertEqual(t, nd, [][2]string{})
		})
	})

	for k, o := range map[string][]byte{
		"sample.jpg":                       []byte("a"),
		"photos/2006/January/sample.jpg":   []byte("ab"),
		"photos/2006/February/sample2.jpg": []byte("abc"),
		"photos/2006/February/sample4.jpg": []byte("abcd"),
		"photos/2006/February/sample5.jpg": []byte("abcde"),
	} {
		AssertEqual(t, impl.PutObject(context.Background(), k, nil, bytes.NewReader(o)), nil)
	}

	t.Run("list all", func(t *testing.T) {
		k, s, p, err := impl.ListObjects(context.Background(), "", "")
		AssertEqual(t, err, nil)
		AssertEqual(t, k, []string{
			"photos/2006/February/sample2.jpg",
			"photos/2006/February/sample4.jpg",
			"photos/2006/February/sample5.jpg",
			"photos/2006/January/sample.jpg",
			"sample.jpg",
		})
		AssertEqual(t, s, []int64{lO + 3, lO + 4, lO + 5, lO + 2, lO + 1})
		AssertEqual(t, len(p), 0)
	})

	t.Run("list by prefix", func(t *testing.T) {
		k, s, p, err := impl.ListObjects(context.Background(), "photos/2006/", "")
		AssertEqual(t, err, nil)
		AssertEqual(t, k, []string{
			"photos/2006/February/sample2.jpg",
			"photos/2006/February/sample4.jpg",
			"photos/2006/February/sample5.jpg",
			"photos/2006/January/sample.jpg",
		})
		AssertEqual(t, s, []int64{lO + 3, lO + 4, lO + 5, lO + 2})
		AssertEqual(t, len(p), 0)
	})

	t.Run("list with delimiter", func(t *testing.T) {
		k, s, p, err := impl.ListObjects(context.Background(), "", "/")
		AssertEqual(t, err, nil)
		AssertEqual(t, k, []string{
			"sample.jpg",
		})
		AssertEqual(t, s, []int64{lO + 1})
		AssertEqual(t, p, []string{"photos/"})
	})

	t.Run("list with prefix and delimiter", func(t *testing.T) {
		k, s, p, err := impl.ListObjects(context.Background(), "photos/2006/", "/")
		AssertEqual(t, err, nil)
		AssertEqual(t, k, []string{})
		AssertEqual(t, s, []int64{})
		AssertEqual(t, p, []string{"photos/2006/February/", "photos/2006/January/"})
	})

	t.Run("put with meta", func(t *testing.T) {
		AssertEqual(t, impl.PutObject(context.Background(), "object/with/meta", map[string]string{"a": "b"}, bytes.NewReader([]byte("example"))), nil)
		n, m, err := impl.HeadObject(context.Background(), "object/with/meta")
		AssertEqual(t, err, nil)
		AssertEqual(t, n, lO+7)
		AssertEqual(t, m["a"], "b")

		buff := bytes.NewBuffer(nil)
		m, err = impl.GetObject(context.Background(), "object/with/meta", buff)
		AssertEqual(t, err, nil)
		AssertEqual(t, m["a"], "b")
		AssertEqual(t, buff.String(), "example")
	})

	t.Run("delete", func(t *testing.T) {
		nd, err := impl.DeleteObjects(context.Background(), []string{"object/with/meta"})
		AssertEqual(t, err, nil)
		AssertEqual(t, nd, [][2]string{})
	})

}

func TestInMemoryS3(t *testing.T) {
	testS3Interface(t, &InMemoryS3{})
}

func TestClientEncryptedS3(t *testing.T) {
	rk := make([]byte, 16)
	_, err := rand.Read(rk)
	AssertEqual(t, err, nil)
	bc, err := aes.NewCipher(rk)
	AssertEqual(t, err, nil)
	testS3Interface(t, &ClientEncryptedS3{S3: &InMemoryS3{}, BlockCipher: bc})
}

// sudo docker run --rm -it -p 4566:4566 --name localstack localstack/localstack
// sudo docker exec localstack awslocal s3api create-bucket --bucket smoke --region us-east-1
// S3_SMOKE_TEST_BUCKET_URL=http://localhost:4566/smoke/ go test -v ./...
func TestS3Api_no_auth(t *testing.T) {
	v := os.Getenv("S3_SMOKE_TEST_BUCKET_URL")
	if v == "" {
		t.Skip("S3_SMOKE_TEST_BUCKET_URL not set")
		return
	}
	u, _ := url.Parse(v)
	testS3Interface(t, NewS3Impl(
		http.DefaultClient,
		u,
	))
}

// sudo docker run --rm -it -p 4566:4566 --name localstack localstack/localstack
// sudo docker exec localstack awslocal s3api create-bucket --bucket smoke --region us-east-1
// S3_SMOKE_TEST_BUCKET_URL=http://localhost:4566/smoke/ go test -v ./...
func TestS3Api_authed(t *testing.T) {
	v := os.Getenv("S3_SMOKE_TEST_BUCKET_URL")
	if v == "" {
		t.Skip("S3_SMOKE_TEST_BUCKET_URL not set")
		return
	}
	u, _ := url.Parse(v)

	client := &http.Client{}
	client.Transport = WrapSigV4RoundTripper(http.DefaultTransport, time.Now, "us-east-1", "fake", "fake")

	testS3Interface(t, NewS3Impl(
		client,
		u,
	))
}

// sudo docker run --rm -it -p 4566:4566 --name localstack localstack/localstack
// sudo docker exec localstack awslocal s3api create-bucket --bucket smoke --region us-east-1
// S3_SMOKE_TEST_BUCKET_URL=http://localhost:4566/smoke/ go test -v ./...
func TestS3Api_encrypted(t *testing.T) {
	v := os.Getenv("S3_SMOKE_TEST_BUCKET_URL")
	if v == "" {
		t.Skip("S3_SMOKE_TEST_BUCKET_URL not set")
		return
	}
	u, _ := url.Parse(v)

	rk := make([]byte, 16)
	_, err := rand.Read(rk)
	AssertEqual(t, err, nil)
	bc, err := aes.NewCipher(rk)
	AssertEqual(t, err, nil)

	testS3Interface(t, &ClientEncryptedS3{
		S3: NewS3Impl(
			http.DefaultClient,
			u,
		),
		BlockCipher: bc,
	})
}

func TestS3Api_aws(t *testing.T) {
	v := os.Getenv("S3_SMOKE_TEST_AWS_BUCKET_URL")
	region := os.Getenv("S3_SMOKE_TEST_AWS_REGION")
	accessKeyId := os.Getenv("S3_SMOKE_TEST_AWS_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("S3_SMOKE_TEST_AWS_SECRET_ACCESS_KEY")
	if v == "" || region == "" || accessKeyId == "" || secretAccessKey == "" {
		t.Skip("S3_SMOKE_TEST_AWS_* not set")
		return
	}
	u, _ := url.Parse(v)

	client := &http.Client{}
	client.Transport = WrapSigV4RoundTripper(http.DefaultTransport, func() time.Time {
		return time.Now().UTC()
	}, region, accessKeyId, secretAccessKey)

	testS3Interface(t, NewS3Impl(
		client,
		u,
	))
}
