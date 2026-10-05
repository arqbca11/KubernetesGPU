# Build context is the repo root (the module lives there):
#   docker build -f shard/Dockerfile -t kgpu-shardsim .
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY experiments ./experiments
COPY shard ./shard
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/shardsim ./shard/cmd/shardsim \
 && mkdir -p /out/timelines

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/shardsim /shardsim
# A named volume mounted here inherits this directory's ownership on first use,
# so the nonroot process can write timelines into it.
COPY --from=build --chown=nonroot:nonroot /out/timelines /timelines
USER nonroot
ENTRYPOINT ["/shardsim"]
