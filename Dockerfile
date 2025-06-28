FROM alpine
COPY rcp-linux /app/rcp
WORKDIR /app/

ENTRYPOINT ["./rcp"]
