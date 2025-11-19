
FROM golang:1.25-alpine

WORKDIR /app

RUN go install github.com/air-verse/air@latest

COPY go.mod go.sum ./
RUN go mod download

COPY .air.toml ./

COPY . .

RUN mkdir -p tmp

CMD ["air", "-c", ".air.toml"]