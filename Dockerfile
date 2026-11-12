# syntax=docker/dockerfile:1

# pt-boost is a single resident Go binary (see docs/design-overview.md
# "部署形态"): this Dockerfile builds it statically in one stage and ships
# only that binary in a minimal, distroless runtime image. No shell, no
# package manager, and in particular no Chromium/headless-browser
# dependency ever enters this image (ADR 0002: M-Team access is direct
# HTTP only).

## ---- build stage ----
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Separate module-download layer: editing source under ./internal or
# ./cmd doesn't bust this layer's cache, only a go.mod/go.sum change does.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/pt-boost ./cmd/pt-boost

## ---- runtime stage ----
# distroless/static has no shell and no package manager, keeping the final
# image to just the binary, its CA certificates (needed for M-Team's
# HTTPS endpoints), and tzdata — nothing resembling a browser or build
# toolchain survives into this stage. :nonroot runs as an unprivileged
# user by default.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

COPY --from=builder /out/pt-boost /pt-boost

# pt-boost never bundles a default config: every site's api_key/category/
# pool_size/etc is deployment-specific (docs/design-overview.md's YAML
# example). Mount your own file at this path — see
# docker-compose.example.yml.
#
# ENTRYPOINT uses exec form, so /pt-boost runs as PID 1 directly with no
# shell in between. That matters for graceful shutdown: `docker stop`
# sends SIGTERM to PID 1, and main.go's signal.NotifyContext only ever
# sees that signal if nothing swallows it first.
EXPOSE 8081
ENTRYPOINT ["/pt-boost", "-config", "/etc/pt-boost/config.yaml"]
