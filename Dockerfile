FROM node:24.13.1-alpine AS web-builder

WORKDIR /build
RUN corepack enable && corepack prepare pnpm@10.24.0 --activate

COPY pnpm-workspace.yaml pnpm-lock.yaml package.json turbo.json ./
COPY apps/web/package.json apps/web/
COPY packages/core/package.json packages/core/
COPY packages/db/package.json packages/db/
COPY packages/redis/package.json packages/redis/
COPY packages/plugin-sdk/package.json packages/plugin-sdk/
COPY packages/email/package.json packages/email/
RUN pnpm install --frozen-lockfile --ignore-scripts

COPY apps/web/ apps/web/
COPY packages/ packages/
RUN pnpm --filter @multica/web build

FROM golang:1.26-alpine AS server-builder

WORKDIR /build/server
RUN apk add --no-cache ca-certificates git
COPY server/go.mod server/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY server/ ./
COPY --from=web-builder /build/apps/web/dist ./internal/static/dist
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -ldflags="-s -w -X main.version=${VERSION:-dev}" -o /multica ./cmd/server

FROM alpine:3.23

RUN apk add --no-cache ca-certificates curl tzdata git openssh-client && \
    addgroup -S multica && adduser -S multica -G multica && \
    mkdir -p /data /app && chown -R multica:multica /data /app

WORKDIR /app
COPY --from=server-builder /multica /app/multica
COPY deploy/docker/docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

USER multica
ENV MULTICA_DB_TYPE=sqlite \
    MULTICA_DB_URL=/data/multica.db \
    MULTICA_HOME=/data \
    MULTICA_PORT=8080

EXPOSE 8080
VOLUME ["/data"]

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["/app/multica"]
