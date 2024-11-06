package automerge_s3_sync

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func signStepByStepTester(
	t *testing.T, builder func() *http.Request, clock func() time.Time, region, keyId, secretKey string,
	expectedCanonicalRequest string,
	expectedStringToSign string,
	expectedSignature string,
	expectedAuthHeader string,
) {
	t.Helper()
	dt := clock()

	var cr string
	var err error
	t.Run("build canonical request", func(t *testing.T) {
		cr, err = buildCanonicalRequest(builder(), dt)
		AssertEqual(t, err, nil)
		AssertEqual(t, cr, expectedCanonicalRequest)
	})

	var sts string
	t.Run("build string to signSigV4", func(t *testing.T) {
		sts = buildStringToSign(dt, region, cr)
		AssertEqual(t, sts, expectedStringToSign)
	})

	var sig string
	t.Run("signature", func(t *testing.T) {
		sig = buildSignature(dt, region, secretKey, sts)
		AssertEqual(t, sig, expectedSignature)
	})

	t.Run("auth header", func(t *testing.T) {
		crParts := strings.Split(cr, "\n")
		ah := buildAuthHeader(dt, region, keyId, crParts[len(crParts)-2], sig)
		AssertEqual(t, ah, expectedAuthHeader)
	})

	t.Run("e2e", func(t *testing.T) {
		r := builder()
		AssertEqual(t, signSigV4(r, clock, region, keyId, secretKey), nil)
		AssertEqual(t, r.Header.Get("Authorization"), expectedAuthHeader)
	})
}

// The PUT example from https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html.
func TestBuildCanonicalRequest_eg_put(t *testing.T) {
	dt := time.Date(2013, 05, 24, 0, 0, 0, 0, time.FixedZone("GMT", 0))
	signStepByStepTester(t, func() *http.Request {
		r, err := http.NewRequest(
			http.MethodPut,
			"https://examplebucket.s3.amazonaws.com/test$file.text",
			strings.NewReader("Welcome to Amazon S3."),
		)
		AssertEqual(t, err, nil)
		r.Header.Set("Date", dt.Format("Mon, 02 Jan 2006 15:04:05 MST"))
		r.Header.Set("x-amz-storage-class", "REDUCED_REDUNDANCY")
		return r
	}, func() time.Time {
		return dt
	}, "us-east-1", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"PUT\n"+
			"/test%24file.text\n"+
			"\n"+
			"date:Fri, 24 May 2013 00:00:00 GMT\n"+
			"host:examplebucket.s3.amazonaws.com\n"+
			"x-amz-content-sha256:44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072\n"+
			"x-amz-date:20130524T000000Z\n"+
			"x-amz-storage-class:REDUCED_REDUNDANCY\n"+
			"\n"+
			"date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class\n"+
			"44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
		"AWS4-HMAC-SHA256\n"+
			"20130524T000000Z\n"+
			"20130524/us-east-1/s3/aws4_request\n"+
			"9e0e90d9c76de8fa5b200d8c849cd5b8dc7a3be3951ddb7f6a76b4158342019d",
		"98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
		"AWS4-HMAC-SHA256 "+
			"Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"+
			"SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,"+
			"Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd",
	)
}

// The GET example from https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html.
func TestBuildCanonicalRequest_eg_get(t *testing.T) {
	signStepByStepTester(t, func() *http.Request {
		r, err := http.NewRequest(
			http.MethodGet,
			"https://examplebucket.s3.amazonaws.com/test.txt",
			nil,
		)
		AssertEqual(t, err, nil)
		r.Header.Set("Range", "bytes=0-9")
		return r
	}, func() time.Time {
		return time.Date(2013, 05, 24, 0, 0, 0, 0, time.FixedZone("GMT", 0))
	}, "us-east-1", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"GET\n"+
			"/test.txt\n"+
			"\n"+
			"host:examplebucket.s3.amazonaws.com\n"+
			"range:bytes=0-9\n"+
			"x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"+
			"x-amz-date:20130524T000000Z\n"+
			"\n"+
			"host;range;x-amz-content-sha256;x-amz-date\n"+
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"AWS4-HMAC-SHA256\n"+
			"20130524T000000Z\n"+
			"20130524/us-east-1/s3/aws4_request\n"+
			"7344ae5b7ee6c3e7e6b0fe0640412a37625d1fbfff95c48bbb2dc43964946972",
		"f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
		"AWS4-HMAC-SHA256 "+
			"Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"+
			"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"+
			"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
	)
}

// The GET bucket lifecycle example from https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html.
func TestBuildCanonicalRequest_eg_get_bucket_lifecycle(t *testing.T) {
	signStepByStepTester(t, func() *http.Request {
		r, err := http.NewRequest(
			http.MethodGet,
			"https://examplebucket.s3.amazonaws.com/?lifecycle",
			nil,
		)
		AssertEqual(t, err, nil)
		return r
	}, func() time.Time {
		return time.Date(2013, 05, 24, 0, 0, 0, 0, time.FixedZone("GMT", 0))
	}, "us-east-1", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"GET\n"+
			"/\n"+
			"lifecycle=\n"+
			"host:examplebucket.s3.amazonaws.com\n"+
			"x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"+
			"x-amz-date:20130524T000000Z\n"+
			"\n"+
			"host;x-amz-content-sha256;x-amz-date\n"+
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"AWS4-HMAC-SHA256\n"+
			"20130524T000000Z\n"+
			"20130524/us-east-1/s3/aws4_request\n"+
			"9766c798316ff2757b517bc739a67f6213b4ab36dd5da2f94eaebf79c77395ca",
		"fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
		"AWS4-HMAC-SHA256 "+
			"Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"+
			"SignedHeaders=host;x-amz-content-sha256;x-amz-date,"+
			"Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543",
	)
}

// The GET list objects from https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html.
func TestBuildCanonicalRequest_eg_get_list_objects(t *testing.T) {
	signStepByStepTester(t, func() *http.Request {
		r, err := http.NewRequest(
			http.MethodGet,
			"https://examplebucket.s3.amazonaws.com/?max-keys=2&prefix=J",
			nil,
		)
		AssertEqual(t, err, nil)
		return r
	}, func() time.Time {
		return time.Date(2013, 05, 24, 0, 0, 0, 0, time.FixedZone("GMT", 0))
	}, "us-east-1", "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"GET\n"+
			"/\n"+
			"max-keys=2&prefix=J\n"+
			"host:examplebucket.s3.amazonaws.com\n"+
			"x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\n"+
			"x-amz-date:20130524T000000Z\n"+
			"\n"+
			"host;x-amz-content-sha256;x-amz-date\n"+
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"AWS4-HMAC-SHA256\n"+
			"20130524T000000Z\n"+
			"20130524/us-east-1/s3/aws4_request\n"+
			"df57d21db20da04d7fa30298dd4488ba3a2b47ca3a489c74750e0f1e7df1b9b7",
		"34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
		"AWS4-HMAC-SHA256 "+
			"Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"+
			"SignedHeaders=host;x-amz-content-sha256;x-amz-date,"+
			"Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7",
	)
}
