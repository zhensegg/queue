FROM golang:1.24-alpine AS api
WORKDIR /src
COPY examples/go-api/go.mod examples/go-api/main.go ./
RUN go build -o /out/zhensegg-api .

FROM zhensegg-bench AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends wrk && rm -rf /var/lib/apt/lists/*
COPY --from=api /out/zhensegg-api /usr/local/bin/zhensegg-api
COPY examples/go-api/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 8080 9091
ENTRYPOINT ["/entrypoint.sh"]
