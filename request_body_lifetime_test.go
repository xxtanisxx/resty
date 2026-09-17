package resty_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"resty.dev/v3"
)

type bodyLifetimeTransport func(*http.Request) (*http.Response, error)

func (transport bodyLifetimeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestBufferedBodyReplay(t *testing.T) {
	for _, scenario := range []string{"retry", "redirect"} {
		t.Run(scenario, func(t *testing.T) {
			seenBodies := make(chan string, 4)
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Errorf("read request: %v", err)
				}
				if request.Method != http.MethodPost {
					t.Errorf("method=%s, want POST", request.Method)
				}
				if request.ContentLength != int64(len(body)) || len(request.TransferEncoding) != 0 {
					t.Errorf("invalid body framing: length=%d, received=%d, transfer=%v", request.ContentLength, len(body), request.TransferEncoding)
				}
				seenBodies <- string(bytes.TrimSpace(body))
				if len(seenBodies) == 1 {
					if scenario == "retry" {
						response.WriteHeader(http.StatusServiceUnavailable)
					} else {
						response.Header().Set("Location", "/redirected")
						response.WriteHeader(http.StatusTemporaryRedirect)
					}
					return
				}
				response.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			client := resty.New()
			defer client.Close()
			if scenario == "retry" {
				client.SetRetryCount(1).
					SetRetryAllowNonIdempotent(true).
					SetRetryWaitTime(time.Nanosecond).
					SetRetryMaxWaitTime(time.Nanosecond).
					AddRetryConditions(func(response *resty.Response, _ error) bool {
						return response != nil && response.StatusCode() == http.StatusServiceUnavailable
					})
			}
			response, err := client.R().
				SetBody(map[string]string{"message": "payload must survive"}).
				Post(server.URL + "/upload")
			if err != nil || response == nil || response.StatusCode() != http.StatusNoContent {
				t.Fatalf("expected success, got response=%v error=%v", response, err)
			}
			if len(seenBodies) != 2 {
				t.Fatalf("saw %d requests, want 2", len(seenBodies))
			}
			for range 2 {
				if body := <-seenBodies; body != `{"message":"payload must survive"}` {
					t.Errorf("replayed body=%q", body)
				}
			}
		})
	}
}

func TestRequestBodySurvivesExecuteReturn(t *testing.T) {
	for _, bodyCase := range []struct {
		name     string
		body     any
		expected string
	}{
		{name: "bytes", body: []byte("payload must survive"), expected: "payload must survive"},
		{name: "string", body: "payload must survive", expected: "payload must survive"},
		{name: "json", body: map[string]string{"message": "payload must survive"}, expected: `{"message":"payload must survive"}`},
	} {
		for _, outcome := range []string{"early_response", "transport_error", "cancellation"} {
			t.Run(bodyCase.name+"/"+outcome, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				allowRead := make(chan struct{})
				finished := make(chan struct{})
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(allowRead) }) }
				var received []byte
				var readErr, closeErr error
				var contentLength int64
				transportErr := errors.New("test transport failure")
				transportCalled := false

				client := resty.NewWithClient(&http.Client{
					Transport: bodyLifetimeTransport(func(request *http.Request) (*http.Response, error) {
						transportCalled = true
						contentLength = request.ContentLength
						t.Cleanup(func() {
							release()
							select {
							case <-finished:
							case <-time.After(5 * time.Second):
								t.Error("transport body reader did not finish")
							}
						})
						go func() {
							defer close(finished)
							<-allowRead
							received, readErr = io.ReadAll(request.Body)
							closeErr = request.Body.Close()
						}()

						if outcome == "transport_error" {
							return nil, transportErr
						}
						if outcome == "cancellation" {
							cancel()
							return nil, request.Context().Err()
						}
						return &http.Response{
							StatusCode: http.StatusUnprocessableEntity,
							Header:     http.Header{"Content-Type": []string{"text/plain"}},
							Body:       io.NopCloser(strings.NewReader("rejected")),
							Request:    request,
						}, nil
					}),
				})
				defer client.Close()

				response, err := client.R().SetContext(ctx).SetBody(bodyCase.body).Post("http://resty.test/upload")
				if !transportCalled {
					t.Fatalf("transport was not called: %v", err)
				}
				switch outcome {
				case "early_response":
					if err != nil || response == nil || response.StatusCode() != http.StatusUnprocessableEntity {
						t.Fatalf("expected an early 422 response, got response=%v error=%v", response, err)
					}
				case "transport_error":
					if !errors.Is(err, transportErr) {
						t.Fatalf("expected transport error, got %v", err)
					}
				case "cancellation":
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("expected cancellation, got %v", err)
					}
				}

				release()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Fatal("transport body reader did not finish")
				}
				if readErr != nil || closeErr != nil {
					t.Fatalf("read error=%v, close error=%v", readErr, closeErr)
				}
				if string(bytes.TrimSpace(received)) != bodyCase.expected {
					t.Errorf("body after Execute returned: got %q, want %q", received, bodyCase.expected)
				}
				if contentLength != int64(len(received)) {
					t.Errorf("Content-Length=%d but transport received %d bytes", contentLength, len(received))
				}
			})
		}
	}
}
