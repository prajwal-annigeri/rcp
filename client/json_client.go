package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"rcp/rcppb"
	"time"
)

type BankOperation struct {
	Operation     string `json:"operation"`
	AccountID     string `json:"account_id,omitempty"`
	SrcAccountID  string `json:"src_account_id,omitempty"`
	DestAccountID string `json:"dest_account_id,omitempty"`
	Amount        *int64 `json:"amount,omitempty"`
}

func readAndSend(filename string, grpcClientMap map[string]rcppb.RCPClient) {
	jsonData, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error reading file %s: %v", filename, err)
	}

	var operations []BankOperation

	err = json.Unmarshal(jsonData, &operations)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON from file %s: %v", filename, err)
	}

	grpcClient := grpcClientMap["S1"]

	log.Println("Processing operations from file:", filename)
	for i, op := range operations {
		go doOp(op, grpcClient, i)
	}
	log.Println("--- Processing complete ---")
}

func doOp(op BankOperation, grpcClient rcppb.RCPClient, i int) {
	log.Printf("--- Operation %d ---\n", i+1)
		switch op.Operation {
		case "get_balance":
			if op.AccountID == "" {
				log.Println("Error: get_balance requires account_id")
				return
			}
			log.Printf("Operation: Get Balance\n")
			log.Printf("Account ID: %s\n", op.AccountID)
			begin := time.Now()
			resp, err := grpcClient.GetBalance(context.Background(), &rcppb.GetBalanceRequest{AccountId: op.AccountID})
			end := time.Since(begin)
			if err != nil {
				log.Printf("Error response: %v", err)
			} else {
				log.Printf("Checking: %d    Savings: %d", resp.CheckingBalance, resp.SavingsBalance)
			}
			
			log.Printf("Time taken: %v", end)

		case "deposit_checking":
			if op.AccountID == "" || op.Amount == nil {
				log.Println("Error: deposit_checking requires account_id and amount")
				return
			}
			log.Printf("Operation: Deposit Checking\n")
			log.Printf("Account ID: %s\n", op.AccountID)
			log.Printf("Amount: %d\n", *op.Amount)
			begin := time.Now()
			_, err := grpcClient.DepositChecking(context.Background(), &rcppb.DepositCheckingRequest{AccountId: op.AccountID, Amount: *op.Amount})
			end := time.Since(begin)
			if err != nil {
				log.Printf("Error response: %v", err)
			}
			log.Printf("Time taken: %v", end)

		case "send_payment":
			if op.SrcAccountID == "" || op.DestAccountID == "" || op.Amount == nil {
				log.Println("Error: send_payment requires src_account_id, dest_account_id, and amount")
				return
			}
			log.Printf("Operation: Send Payment\n")
			log.Printf("Source Account ID: %s\n", op.SrcAccountID)
			log.Printf("Destination Account ID: %s\n", op.DestAccountID)
			log.Printf("Amount: %d\n", *op.Amount)
			begin := time.Now()
			_, err := grpcClient.SendPayment(context.Background(), &rcppb.SendPaymentRequest{AccountIdFrom: op.SrcAccountID, AccountIdTo: op.DestAccountID, Amount: *op.Amount})
			end := time.Since(begin)
			if err != nil {
				log.Printf("Error response: %v", err)
			}
			log.Printf("Time taken: %v", end)

		case "write_check":
			if op.AccountID == "" || op.Amount == nil {
				log.Println("Error: write_check requires account_id and amount")
				return
			}
			log.Printf("Operation: Write Check\n")
			log.Printf("Account ID: %s\n", op.AccountID)
			log.Printf("Amount: %d\n", *op.Amount)
			begin := time.Now()
			_, err := grpcClient.WriteCheck(context.Background(), &rcppb.WriteCheckRequest{AccountId: op.AccountID, Amount: *op.Amount})
			end := time.Since(begin)
			if err != nil {
				log.Printf("Error response: %v", err)
			}
			log.Printf("Time taken: %v", end)
			

		case "amalgamate":
			if op.SrcAccountID == "" || op.DestAccountID == "" {
				log.Println("Error: amalgamate requires src_account_id and dest_account_id")
				return
			}
			log.Printf("Operation: Amalgamate\n")
			log.Printf("Source Account ID: %s\n", op.SrcAccountID)
			log.Printf("Destination Account ID: %s\n", op.DestAccountID)
			begin := time.Now()
			_, err := grpcClient.Amalgamate(context.Background(), &rcppb.AmalgamateRequest{AccountIdFrom: op.SrcAccountID, AccountIdTo: op.DestAccountID})
			end := time.Since(begin)
			if err != nil {
				log.Printf("Error response: %v", err)
			}
			log.Printf("Time taken: %v", end)

		case "transact_savings":
			if op.SrcAccountID == "" || op.Amount == nil {
				log.Println("Error: transact_savings requires src_account_id and amount")
				return
			}
			log.Printf("Operation: Transact Savings\n")
			log.Printf("Source Account ID: %s\n", op.SrcAccountID)
			log.Printf("Amount: %d\n", *op.Amount)
			begin := time.Now()
			_, err := grpcClient.TransactSavings(context.Background(), &rcppb.TransactSavingsRequest{AccountId: op.AccountID, Amount: *op.Amount})
			end := time.Since(begin)
			if err != nil {
				log.Printf("Error response: %v", err)
			}
			log.Printf("Time taken: %v", end)

		default:
			log.Printf("Unknown operation: %s\n", op.Operation)
		}
}
