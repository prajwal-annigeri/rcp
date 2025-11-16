protoc \
  --proto_path=proto \
  --go_out=grpc \
  --go-grpc_out=grpc \
  proto/*.proto
