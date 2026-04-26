FROM golang:1.21 as builder

WORKDIR /workspace

COPY go.sum go.sum
COPY go.mod go.mod
COPY vendor/ vendor/

COPY cmd/ cmd/
COPY dialer/ dialer/
COPY registry/ registry/
COPY sandbox/ sandbox/
COPY services/ services/
COPY tls/ tls/
COPY tracing/ tracing/
COPY tune/ tune/
COPY x509/ x509/

COPY config.json config.json
RUN mkdir -p /out
ENV GOBIN=/out
RUN CGO_ENABLED=0 GOOS=linux GO111MODULE=on go install -ldflags="-s -w" -mod=vendor ./cmd/...


FROM alpine:latest

RUN apk --no-cache add ca-certificates
WORKDIR /go/bin
COPY --from=builder /out/* /go/bin/
COPY --from=builder /workspace/config.json /go/bin/config.json
COPY --from=builder /workspace/x509 /go/bin/x509

CMD ["./frontend"]
