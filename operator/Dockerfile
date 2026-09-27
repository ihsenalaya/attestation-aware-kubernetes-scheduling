# Build the manager binary
FROM docker.io/library/golang:1.26.5@sha256:079e59808d2d252516e27e3f3a9c003740dee7f75e55aa71528766d52bcfc16a AS builder
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o manager ./cmd/main.go
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -buildvcs=false -ldflags='-s -w -buildid=' -o header-proxy ./cmd/header-proxy/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot@sha256:d29e660cc75a5b6b1334e03c5c81ccf9bc0884a002c6000dbf0fb96034814478
WORKDIR /
COPY --from=builder /workspace/manager .
COPY --from=builder /workspace/header-proxy .
USER 65532:65532

ENTRYPOINT ["/manager"]
