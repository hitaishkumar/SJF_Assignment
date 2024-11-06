# Use an official Golang image as a build stage
FROM golang:1.23 as builder

# Set the working directory inside the container
WORKDIR /app

# Copy the Go code to the container
COPY . .

# Build the Go application
RUN go mod download
RUN go build -o go-backend .

# Use a smaller base image for the final stage
FROM alpine:latest

# Copy the binary from the builder stage
COPY --from=builder /app/go-backend /go-backend

# Expose port 8080 for the Go backend
EXPOSE 8080

# Run the Go backend
CMD ["/go-backend"]
