package automerge_s3_sync

import (
	"bytes"
	"context"
	"io"
	"testing"
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
			AssertEqual(t, err.Error(), "object not found")
			AssertEqual(t, m, nil)
			AssertEqual(t, n, 0)
		})
		t.Run("get", func(t *testing.T) {
			m, err := impl.GetObject(context.Background(), "thing", io.Discard)
			AssertEqual(t, err.Error(), "object not found")
			AssertEqual(t, m, nil)
		})
		t.Run("delete", func(t *testing.T) {
			nd, err := impl.DeleteObjects(context.Background(), []string{"thing"})
			AssertEqual(t, err, nil)
			AssertEqual(t, nd, [][2]string{{"thing", "object not found"}})
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
		AssertEqual(t, s, []int64{3, 4, 5, 2, 1})
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
		AssertEqual(t, s, []int64{3, 4, 5, 2})
		AssertEqual(t, len(p), 0)
	})

	t.Run("list with delimiter", func(t *testing.T) {
		k, s, p, err := impl.ListObjects(context.Background(), "", "/")
		AssertEqual(t, err, nil)
		AssertEqual(t, k, []string{
			"sample.jpg",
		})
		AssertEqual(t, s, []int64{1})
		AssertEqual(t, p, []string{"photos"})
	})

	t.Run("list with prefix and delimiter", func(t *testing.T) {
		k, s, p, err := impl.ListObjects(context.Background(), "photos/2006/", "/")
		AssertEqual(t, err, nil)
		AssertEqual(t, k, []string{})
		AssertEqual(t, s, []int64{})
		AssertEqual(t, p, []string{"February", "January"})
	})

}

func TestInMemoryS3(t *testing.T) {
	testS3Interface(t, &InMemoryS3{})
}
