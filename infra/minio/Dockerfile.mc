FROM golang:1.24.8-alpine AS build

ARG MC_VERSION=RELEASE.2025-08-13T08-35-41Z
ENV CGO_ENABLED=0

RUN for attempt in 1 2 3; do \
      go install -trimpath "github.com/minio/mc@${MC_VERSION}" && exit 0; \
      echo "mc source build attempt ${attempt} failed; retrying after a short delay"; \
      sleep 5; \
    done; \
    exit 1

FROM alpine:3.22

RUN apk add --no-cache ca-certificates
COPY --from=build /go/bin/mc /usr/local/bin/mc

ENTRYPOINT ["mc"]
