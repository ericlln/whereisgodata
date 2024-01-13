FROM golang:1.21-alpine AS Build

WORKDIR /app

# Copy only the necessary files for go mod download
COPY go.mod .
COPY go.sum .

# Download dependencies
RUN go mod download

# Copy the entire project into the container
COPY . .

# Build the Go application
RUN go build -o /whereisgodata /app/cmd/whereisgodata/whereisgodata.go

FROM alpine:latest

WORKDIR /

# Copy the binary from the BuildStage
COPY --from=Build /whereisgodata /whereisgodata

EXPOSE 50051

ENTRYPOINT ["/whereisgodata"]