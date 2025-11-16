protoc \
  --proto_path=proto \
  --go_out=. \
  --go-grpc_out=. \
  proto/*.proto
