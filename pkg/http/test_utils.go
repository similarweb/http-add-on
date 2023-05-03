package http

import (
	nethttp "net/http"
	url "net/url"
	"net/http/httptest"
	"strings"
)

func NewTestCtx(
	method,
	path string,
) (*nethttp.Request, *httptest.ResponseRecorder) {
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	return req, rec
}

// GetUrlFromHostAndPath return a URL from schemeless host and path
func GetUrlFromHostAndPath(hostAndPath string) url.URL {
	mockScheme := "http://"
	urlStr := mockScheme + hostAndPath
	url, _ := url.Parse(urlStr)

	return *url
}

func CleanPath(path string) string {
	cleanPath := strings.TrimLeft(path, "/")

	return cleanPath

}

func GetHostFromHostAndPath(hostAndPath string) string {
	slices := strings.Split(hostAndPath, "/")
	return slices[0]
}

func GetPathFromHostAndPath(hostAndPath string) string {
	url, err := url.Parse(hostAndPath)
	if err != nil {
		return ""
	}
	
	path := strings.Split(hostAndPath, "/")[0] + url.Path
	return path
}