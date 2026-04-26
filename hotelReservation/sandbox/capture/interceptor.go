package capture

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	envCapturePath    = "DSB_CAPTURE_PATH"
	envCaptureService = "DSB_CAPTURE_SERVICE"
)

var fileLocks sync.Map

type record struct {
	Timestamp  string          `json:"timestamp"`
	Service    string          `json:"service"`
	Method     string          `json:"method"`
	Request    json.RawMessage `json:"request,omitempty"`
	Response   json.RawMessage `json:"response,omitempty"`
	Code       string          `json:"code"`
	Error      string          `json:"error,omitempty"`
	DurationMs int64           `json:"duration_ms"`
}

// UnaryServerInterceptorFromEnv enables NDJSON request/response capture when
// DSB_CAPTURE_PATH is set. When unset, it returns a no-op interceptor.
func UnaryServerInterceptorFromEnv() grpc.UnaryServerInterceptor {
	capturePath := os.Getenv(envCapturePath)
	if capturePath == "" {
		return func(
			ctx context.Context,
			req interface{},
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (interface{}, error) {
			return handler(ctx, req)
		}
	}

	serviceName := os.Getenv(envCaptureService)
	if serviceName == "" {
		serviceName = "unknown"
	}

	if err := os.MkdirAll(filepath.Dir(capturePath), 0o755); err != nil {
		log.Error().Err(err).Msg("capture: failed to create capture directory")
		return func(
			ctx context.Context,
			req interface{},
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (interface{}, error) {
			return handler(ctx, req)
		}
	}

	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		rec := record{
			Timestamp:  start.UTC().Format(time.RFC3339Nano),
			Service:    serviceName,
			Method:     info.FullMethod,
			Request:    mustMarshal(req),
			Response:   mustMarshal(resp),
			Code:       status.Code(err).String(),
			DurationMs: time.Since(start).Milliseconds(),
		}
		if err != nil {
			rec.Error = err.Error()
		}

		if writeErr := appendRecord(capturePath, rec); writeErr != nil {
			log.Error().Err(writeErr).Msg("capture: failed to append record")
		}

		return resp, err
	}
}

func mustMarshal(value interface{}) json.RawMessage {
	if value == nil {
		return nil
	}
	msg, ok := value.(proto.Message)
	if !ok {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		return raw
	}
	raw, err := protojson.Marshal(msg)
	if err != nil {
		return nil
	}
	return raw
}

func appendRecord(path string, rec record) error {
	lock, _ := fileLocks.LoadOrStore(path, &sync.Mutex{})
	mutex := lock.(*sync.Mutex)

	mutex.Lock()
	defer mutex.Unlock()

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	return enc.Encode(rec)
}
