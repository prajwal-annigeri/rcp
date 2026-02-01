module github.com/pingcap/go-ycsb

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2
	github.com/chzyer/readline v0.0.0-20180603132655-2972be24d48e
	github.com/magiconair/properties v1.8.0
	github.com/olekukonko/tablewriter v0.0.5
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c
	github.com/spf13/cobra v1.0.0
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
)

require (
	google.golang.org/grpc v1.76.0
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/chzyer/test v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/text v0.27.0 // indirect
)

replace github.com/apache/thrift => github.com/apache/thrift v0.0.0-20171203172758-327ebb6c2b6d

go 1.24.0

toolchain go1.24.10
