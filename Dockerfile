# syntax=docker/dockerfile:1
FROM golang:1.17.7 as builder

WORKDIR /aggregator
COPY . .
RUN go mod tidy
RUN go get -d -v ./...
RUN GOOS=linux GOARCH=amd64 go build -o aggregator ./

FROM gcr.io/distroless/base-debian11

WORKDIR /aggregator
WORKDIR ./bin
COPY --from=builder /aggregator/aggregator ./

CMD ["/aggregator/bin/aggregator"]