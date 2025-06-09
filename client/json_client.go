package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"rcp/rcppb"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

type OperationStruct struct {
	Operation     string `json:"operation"`
	AccountID     string `json:"account_id,omitempty"`
	SrcAccountID  string `json:"src_account_id,omitempty"`
	DestAccountID string `json:"dest_account_id,omitempty"`
	Amount        *int64 `json:"amount,omitempty"`
	NodeID        string `json:"node_id"`
	SrcNodeID     string `json:"src_node_id"`
	DstNodeID     string `json:"dst_node_id"`
	Delay         *int64 `json:"delay"`
	Key           string `json:"key"`
	Value         string `json:"value"`
	Bucket        string `json:"bucket"`
}

var (
	opsCount     int
	totalTime    time.Duration
	latencySlice []int64
	endTime      time.Time
	beginTime    time.Time
)

func readAndSend(filename string, grpcClientMap map[string]rcppb.RCPClient) {
	// opsCount = 0
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error reading file %s: %v", filename, err)
	}

	var operations []OperationStruct

	err = json.Unmarshal(jsonData, &operations)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON from file %s: %v", filename, err)
	}
	log.Println("Processing operations from file:", filename)
	opsCount = len(operations)
	beginTime = time.Now()
	for i, op := range operations {
		go doOp(op, grpcClientMap, i)
		// time.Sleep(10 * time.Millisecond)
	}
	// writeDurationsToFile()
	log.Println("--- Processing complete ---")
}

func doOp(op OperationStruct, grpcClientMap map[string]rcppb.RCPClient, i int) {
	log.Printf("--- Operation %d ---\n", i)
	var begin time.Time
	var dur time.Duration
	switch op.Operation {

	case "KV_Store":
		if op.NodeID == "" {
			log.Println("Error: no node_id")
			return
		}
		log.Printf("Operation: KV Store Key: %s, Value: %s, Bucket: %s to node %s\n", op.Key, op.Value, op.Bucket, op.NodeID)
		grpcClient, ok := grpcClientMap[op.NodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		begin := time.Now()
		_, err := grpcClient.Store(context.Background(), &rcppb.StoreRequest{Key: op.Key, Value: op.Value})
		if err != nil {
			log.Printf("Error with store: %v", err)
		} else {
			t := time.Now()
			log.Printf("Time taken op %d: %v\n", i, t.Sub(begin))
			go calcThroughPut(i, t)
		}
	case "kill":
		if op.NodeID == "" {
			log.Println("Error: no node_id")
			return
		}
		log.Printf("Operation: Kill %s\n", op.NodeID)
		grpcClient, ok := grpcClientMap[op.NodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		grpcClient.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: false})
	case "revive":
		if op.NodeID == "" {
			log.Println("Error: no node_id")
			return
		}
		log.Printf("Operation: Revive %s\n", op.NodeID)
		grpcClient, ok := grpcClientMap[op.NodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		grpcClient.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: true})
	case "delay":
		if op.SrcNodeID == "" || op.DstNodeID == "" {
			log.Println("No src_node_id or dst_node_id")
			return
		}
		log.Printf("Operation set delay from %s to %s: %dms", op.SrcNodeID, op.DstNodeID, *op.Delay)
		grpcClient, ok := grpcClientMap[op.SrcNodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		_, err := grpcClient.Delay(context.Background(), &rcppb.DelayRequest{NodeId: op.DstNodeID, Delay: *op.Delay})
		if err != nil {
			log.Printf("Error response: %v", err)
		} else {
			log.Printf("Set delay %s -> %s %dms", op.SrcNodeID, op.DstNodeID, *op.Delay)
		}
	case "get_balance":
		if op.AccountID == "" {
			log.Println("Error: get_balance requires account_id")
			return
		}
		if op.NodeID == "" {
			log.Println("Error: no node_id")
			return
		}
		log.Printf("Operation: Get Balance\n")
		log.Printf("Account ID: %s\n", op.AccountID)
		grpcClient, ok := grpcClientMap[op.NodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		begin = time.Now()
		resp, err := grpcClient.GetBalance(context.Background(), &rcppb.GetBalanceRequest{AccountId: op.AccountID})
		dur = time.Since(begin)
		if err != nil {
			log.Printf("Error response: %v", err)
		} else {
			log.Printf("Checking: %d    Savings: %d", resp.CheckingBalance, resp.SavingsBalance)
		}

		log.Printf("Time taken op %d: %v", i, dur)
		latencySlice = append(latencySlice, dur.Milliseconds())
		// go updateMetric(end)
	case "deposit_checking":
		if op.AccountID == "" || op.Amount == nil || op.NodeID == "" {
			log.Println("Error: deposit_checking requires account_id, node_id and amount")
			return
		}
		log.Printf("Operation: Deposit Checking\n")
		log.Printf("Account ID: %s\n", op.AccountID)
		log.Printf("Amount: %d\n", *op.Amount)
		grpcClient, ok := grpcClientMap[op.NodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		begin = time.Now()
		_, err := grpcClient.DepositChecking(context.Background(), &rcppb.DepositCheckingRequest{AccountId: op.AccountID, Amount: *op.Amount})
		dur = time.Since(begin)
		if err != nil {
			log.Printf("Error response: %v", err)
		}

		latencySlice = append(latencySlice, dur.Milliseconds())
		go updateMetric(dur)
	case "send_payment":
		if op.SrcAccountID == "" || op.DestAccountID == "" || op.Amount == nil {
			log.Println("Error: send_payment requires src_account_id, dest_account_id, and amount")
			return
		}
		log.Printf("Operation: Send Payment\n")
		log.Printf("Source Account ID: %s\n", op.SrcAccountID)
		log.Printf("Destination Account ID: %s\n", op.DestAccountID)
		log.Printf("Amount: %d\n", *op.Amount)
		grpcClient, ok := grpcClientMap[op.NodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		begin = time.Now()
		_, err := grpcClient.SendPayment(context.Background(), &rcppb.SendPaymentRequest{AccountIdFrom: op.SrcAccountID, AccountIdTo: op.DestAccountID, Amount: *op.Amount})
		dur = time.Since(begin)
		if err != nil {
			log.Printf("Error response: %v", err)
		}
		latencySlice = append(latencySlice, dur.Milliseconds())
		go updateMetric(dur)
	case "write_check":
		if op.AccountID == "" || op.Amount == nil {
			log.Println("Error: write_check requires account_id and amount")
			return
		}
		log.Printf("Operation: Write Check\n")
		log.Printf("Account ID: %s\n", op.AccountID)
		log.Printf("Amount: %d\n", *op.Amount)
		grpcClient, ok := grpcClientMap[op.NodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		begin = time.Now()
		_, err := grpcClient.WriteCheck(context.Background(), &rcppb.WriteCheckRequest{AccountId: op.AccountID, Amount: *op.Amount})
		dur = time.Since(begin)
		if err != nil {
			log.Printf("Error response: %v", err)
		}
		latencySlice = append(latencySlice, dur.Milliseconds())
		go updateMetric(dur)

	case "amalgamate":
		if op.SrcAccountID == "" || op.DestAccountID == "" {
			log.Println("Error: amalgamate requires src_account_id and dest_account_id")
			return
		}
		log.Printf("Operation: Amalgamate\n")
		log.Printf("Source Account ID: %s\n", op.SrcAccountID)
		log.Printf("Destination Account ID: %s\n", op.DestAccountID)
		grpcClient, ok := grpcClientMap[op.NodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		begin = time.Now()
		_, err := grpcClient.Amalgamate(context.Background(), &rcppb.AmalgamateRequest{AccountIdFrom: op.SrcAccountID, AccountIdTo: op.DestAccountID})
		dur = time.Since(begin)
		if err != nil {
			log.Printf("Error response: %v", err)
		}
		latencySlice = append(latencySlice, dur.Milliseconds())
		go updateMetric(dur)
	case "transact_savings":
		if op.AccountID == "" || op.Amount == nil {
			log.Println("Error: transact_savings requires src_account_id and amount")
			return
		}
		log.Printf("Operation: Transact Savings\n")
		log.Printf("Source Account ID: %s\n", op.SrcAccountID)
		log.Printf("Amount: %d\n", *op.Amount)
		grpcClient, ok := grpcClientMap[op.NodeID]
		if !ok {
			log.Printf("No gRPC client for %s", op.NodeID)
			return
		}
		begin = time.Now()
		_, err := grpcClient.TransactSavings(context.Background(), &rcppb.TransactSavingsRequest{AccountId: op.AccountID, Amount: *op.Amount})
		dur = time.Since(begin)
		if err != nil {
			log.Printf("Error response: %v", err)
		}
		latencySlice = append(latencySlice, dur.Milliseconds())
		go updateMetric(dur)

	default:
		log.Printf("Unknown operation: %s\n", op.Operation)
	}
	// log.Printf("Time taken op %d: %v", i, dur)
}

var mutex sync.Mutex

func updateMetric(timeTaken time.Duration) {
	mutex.Lock()
	defer mutex.Unlock()
	opsCount += 1
	totalTime += timeTaken
}

func printMetric() {
	log.Printf("%v", totalTime/time.Duration(opsCount))
}

func writeDurationsToFile() error {
	// Create or truncate the file
	filename := fmt.Sprintf("%s.txt", time.Now().String())
	file, err := os.Create(filename)
	slices.Sort(latencySlice)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filename, err)
	}
	defer file.Close() // Ensure file is closed when function returns

	// Create a new writer
	writer := bufio.NewWriter(file)

	// Write each duration to the file
	for i, d := range latencySlice {
		// Convert duration to string (e.g., "5s", "1m30s")
		// The String() method of time.Duration provides a convenient format.
		_, err := fmt.Fprintf(writer, "%d,", d)
		if err != nil {
			return fmt.Errorf("failed to write duration %d (%v) to file: %w", i, d, err)
		}
	}

	// Flush ensures all buffered data is written to the file
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	fmt.Printf("Successfully wrote %d durations to %s\n", len(latencySlice), filename)
	return nil
}

var mutexTP sync.Mutex

func calcThroughPut(opNum int, endT time.Time) {
	mutexTP.Lock()
	defer mutexTP.Unlock()
	if endTime.Before(endT) {
		endTime = endT
	}
}

func printTP() {
	log.Printf("Time taken %v, ops: %d\n", endTime.Sub(beginTime), opsCount)
	log.Printf("Throughput: %.2f", float64(opsCount)/endTime.Sub(beginTime).Seconds())
}
