# Build context is the repo root (the module lives there):
#   docker build -f scheduler/Dockerfile -t kgpu-scheduler .
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY db ./db
COPY scheduler ./scheduler
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/scheduler ./scheduler/cmd/scheduler

# distroless/static: no shell, no package manager, just the binary and CA certs.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/scheduler /scheduler
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/scheduler"]
