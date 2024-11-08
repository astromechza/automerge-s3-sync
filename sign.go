package automerge_s3_sync

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// signSigV4 appends a AWS sigv4 signature to the request according to the reference at
// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html.
func signSigV4(r *http.Request, clock func() time.Time, region, keyId, secretKey string) error {
	t := clock()
	if cr, err := buildCanonicalRequest(r, t); err != nil {
		return err
	} else {
		crParts := strings.Split(cr, "\n")
		r.Header.Set("Authorization", buildAuthHeader(
			t, region, keyId, crParts[len(crParts)-2], buildSignature(
				t, region, secretKey,
				buildStringToSign(t, region, cr),
			),
		))
	}
	return nil
}

// uriEncode is an implementation of the uri encoding function as specified in https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html.
func uriEncode(in string, out io.Writer) {
	for _, bb := range []byte(in) {
		if (bb >= 'A' && bb <= 'Z') || (bb >= 'a' && bb <= 'z') || (bb >= '0' && bb <= '9') || bb == '-' || bb == '_' || bb == '.' || bb == '~' {
			_, _ = out.Write([]byte{bb})
		} else {
			_, _ = out.Write([]byte{'%'})
			hb := make([]byte, 2)
			_ = hex.Encode(hb, []byte{bb})
			for i, b := range hb {
				if b >= 'a' && b <= 'f' {
					hb[i] = b - 32
				}
			}
			_, _ = out.Write(hb)
		}
	}
}

func buildCanonicalRequest(r *http.Request, t time.Time) (string, error) {
	r.Header.Set("x-amz-date", t.UTC().Format("20060102T150405Z"))
	r.Header.Set("Host", r.Host)
	contentSha256 := r.Header.Get("x-amz-checksum-sha256")
	if contentSha256 == "" {
		h := sha256.New()
		if r.Body != nil {
			if buff, err := io.ReadAll(r.Body); err != nil {
				return "", fmt.Errorf("failed read body to buffer for signing: %w", err)
			} else {
				_, _ = h.Write(buff)
				r.Body = io.NopCloser(bytes.NewReader(buff))
			}
		}
		contentSha256 = fmt.Sprintf("%x", h.Sum(nil))
	}
	r.Header.Set("x-amz-content-sha256", contentSha256)

	sb := new(strings.Builder)
	sb.WriteString(r.Method)
	sb.WriteRune('\n')

	for i, part := range strings.Split(r.URL.Path, "/") {
		if i > 0 {
			sb.WriteRune('/')
		}
		uriEncode(part, sb)
	}

	sb.WriteRune('\n')
	qv := r.URL.Query()
	queryKeys := make([]string, 0, len(qv))
	for k := range r.URL.Query() {
		queryKeys = append(queryKeys, k)
	}
	sort.Strings(queryKeys)
	for i, k := range queryKeys {
		if i > 0 {
			sb.WriteRune('&')
		}
		uriEncode(k, sb)
		sb.WriteRune('=')
		uriEncode(qv.Get(k), sb)
	}
	sb.WriteRune('\n')
	headerKeys := make([]string, 0, len(r.Header))
	for k := range r.Header {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-amz-") || k == "content-md5" || k == "date" || k == "host" || k == "range" {
			headerKeys = append(headerKeys, k)
		}
	}
	sort.Strings(headerKeys)
	for _, k := range headerKeys {
		sb.WriteString(k)
		sb.WriteRune(':')
		sb.WriteString(r.Header.Get(k))
		sb.WriteRune('\n')
	}
	sb.WriteRune('\n')
	for i, key := range headerKeys {
		if i > 0 {
			sb.WriteRune(';')
		}
		sb.WriteString(key)
	}
	sb.WriteRune('\n')
	sb.WriteString(contentSha256)
	return sb.String(), nil
}

func hmacSha(key []byte, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func buildStringToSign(t time.Time, region string, canonicalRequest string) string {
	stringToSign := new(strings.Builder)
	stringToSign.WriteString("AWS4-HMAC-SHA256\n")
	stringToSign.WriteString(t.UTC().Format("20060102T150405Z"))
	stringToSign.WriteRune('\n')
	stringToSign.WriteString(t.UTC().Format("20060102"))
	stringToSign.WriteRune('/')
	stringToSign.WriteString(region)
	stringToSign.WriteString("/s3/aws4_request\n")
	h := sha256.New()
	h.Write([]byte(canonicalRequest))
	stringToSign.WriteString(hex.EncodeToString(h.Sum(nil)))
	return stringToSign.String()
}

func buildSignature(t time.Time, region, secretKey, stringToSign string) string {
	hash := hmacSha([]byte("AWS4"+secretKey), []byte(t.UTC().Format("20060102")))
	hash = hmacSha(hash, []byte(region))
	hash = hmacSha(hash, []byte("s3"))
	hash = hmacSha(hash, []byte("aws4_request"))
	return hex.EncodeToString(hmacSha(hash, []byte(stringToSign)))
}

func buildAuthHeader(t time.Time, region, keyId, signedHeaders, signature string) string {
	authHeader := new(strings.Builder)
	authHeader.WriteString("AWS4-HMAC-SHA256 Credential=")
	authHeader.WriteString(keyId)
	authHeader.WriteRune('/')
	authHeader.WriteString(t.UTC().Format("20060102"))
	authHeader.WriteRune('/')
	authHeader.WriteString(region)
	authHeader.WriteString("/s3/aws4_request,SignedHeaders=")
	authHeader.WriteString(signedHeaders)
	authHeader.WriteString(",Signature=")
	authHeader.WriteString(signature)
	return authHeader.String()
}

type HttpRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f HttpRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// WrapSigV4RoundTripper wraps a http client with a signer
func WrapSigV4RoundTripper(rt http.RoundTripper, clock func() time.Time, region, keyId, secretKey string) HttpRoundTripperFunc {
	return HttpRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if err := signSigV4(request, clock, region, keyId, secretKey); err != nil {
			return nil, err
		}
		return rt.RoundTrip(request)
	})
}
