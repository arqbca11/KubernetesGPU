# Build context is the repo root (the module lives there):
#   docker build -f shard/Dockerfile -t kgpu-shardsim .
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY experiments ./experiments
COPY shard ./shard
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/shardsim ./shard/cmd/shardsim

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/shardsim /shardsim
USER nonroot
ENTRYPOINT ["/shardsim"]
