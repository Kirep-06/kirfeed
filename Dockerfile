FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kirfeed-api ./cmd \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kirfeed-worker ./cmd/worker

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && addgroup -S app \
    && adduser -S -G app app

WORKDIR /app

COPY --from=builder --chown=app:app /out/kirfeed-api ./kirfeed-api
COPY --from=builder --chown=app:app /out/kirfeed-worker ./kirfeed-worker
COPY --chown=app:app configs ./configs
RUN mkdir -p /app/.run/uploads && chown -R app:app /app/.run

USER app

EXPOSE 8080
CMD ["./kirfeed-api"]
