# Build static binaries. Without a local Go toolchain, `make docker-build`
# runs the same build in the official golang image.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GO_IMAGE ?= golang:1.25
DIST := dist

.PHONY: build test docker-build docker-test clean

build:
	mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags '$(LDFLAGS)' -o $(DIST)/snapshot-finder-linux-amd64 ./cmd/snapshot-finder
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags '$(LDFLAGS)' -o $(DIST)/snapshot-finder-linux-arm64 ./cmd/snapshot-finder
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags '-s -w' -o $(DIST)/fakecluster-linux-amd64 ./cmd/fakecluster
	cd $(DIST) && sha256sum snapshot-finder-linux-* > SHA256SUMS

test:
	go vet ./...
	go test -race -count=1 ./...

docker-build:
	docker run --rm -v $(CURDIR):/src -w /src -e GOFLAGS=-buildvcs=false $(GO_IMAGE) make build VERSION=$(VERSION)

docker-test:
	docker run --rm -v $(CURDIR):/src -w /src -e GOFLAGS=-buildvcs=false $(GO_IMAGE) make test

clean:
	rm -rf $(DIST)
