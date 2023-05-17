package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/go-logr/logr"

	"github.com/kedacore/http-add-on/pkg/queue"
	"github.com/kedacore/http-add-on/pkg/routing"
)

func getHost(r *http.Request) (string, error) {
	// check the host header first, then the request host
	// field (which may contain the actual URL if there is no
	// host header)
	if r.Header.Get("Host") != "" {
		return r.Header.Get("Host"), nil
	}
	if r.Host != "" {
		return r.Host, nil
	}
	return "", fmt.Errorf("host not found")
}

// countMiddleware adds 1 to the given queue counter, executes next
// (by calling ServeHTTP on it), then decrements the queue counter
func countMiddleware(
	lggr logr.Logger,
	q queue.Counter,
	routingTable routing.Table,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _ := getHost(r)
		url := *r.URL
		url.Host = host
		httpso := routingTable.Route(r)
		if httpso == nil {
			log.Printf("Error looking up route for Host Pattern %s%s, not forwarding request", url.Host, url.Path)
			w.WriteHeader(400)
			if _, err := w.Write([]byte("Host not found, not forwarding request")); err != nil {
				lggr.Error(err, "could not write error message to client")
			}
			return
		}
		hostPathPattrn := httpso.Spec.Host + httpso.Spec.Path
		if err := q.Resize(hostPathPattrn, +1); err != nil {
			log.Printf("Error incrementing queue for %q (%s)", r.RequestURI, err)
		}
		defer func() {
			if err := q.Resize(hostPathPattrn, -1); err != nil {
				log.Printf("Error decrementing queue for %q (%s)", r.RequestURI, err)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
