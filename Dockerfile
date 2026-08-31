FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/fable-server ./cmd/my_chat_server

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/fable-server /app/fable-server
COPY config/config.example.toml /app/config/config.toml
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/fable-server"]
