package node

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"rcp/rcppb"
)

type StoreRequest struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Bucket string `json:"bucket"`
}

type StoreResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type GetKVResponse struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
}

type DeleteResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Success bool   `json:"success"`
}

func (node *Node) startHttpServer() {
	log.Printf("Starting HTTP server on port %s", node.HttpPort)
	http.HandleFunc("/put", node.putHandler)
	http.HandleFunc("/get", node.getHandler)
	http.HandleFunc("/delete", node.deleteHandler)
	err := http.ListenAndServe(node.HttpPort, nil)
	if err != nil {
		log.Printf("Failed to start HTTP server: %v", err)
	}
}

func (node *Node) putHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		node.sendError(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	

	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	bucket := r.URL.Query().Get("bucket")

	log.Printf("Got %s, %s, %s", key, value, bucket)
	if key == "" || value == "" {
		node.sendError(w, "'key' and 'value'required", http.StatusBadRequest)
		return
	}

	if bucket == "" {
		bucket = DefaultBucket
	}

	log.Printf("Storing: Key=%s, Value=%s", key, value)

	_, err := node.Store(context.Background(), &rcppb.StoreRequest{Key: key, Value: value, Bucket: bucket})
	if err != nil {
		log.Printf("Store failed: %v", err)
		node.sendError(w, "Store operation failed", http.StatusInternalServerError)
		return
	}

	node.sendJSON(w, StoreResponse{Success: true, Message: "Stored successfully"})
}

func (node *Node) getHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		node.sendError(w, "Only GET allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	bucket := r.URL.Query().Get("bucket")
	if key == "" {
		node.sendError(w, "Missing 'key'", http.StatusBadRequest)
		return
	}

	if bucket == "" {
		bucket = DefaultBucket
	}

	value, err := node.Get(context.Background(), &rcppb.GetValueReq{Key: key, Bucket: bucket})
	if err != nil {
		log.Printf("Get failed: %v", err)
		node.sendJSON(w, GetKVResponse{Value: "", Found: false})
		return
	}

	node.sendJSON(w, GetKVResponse{Value: value.Value, Found: true})
}

func (node *Node) deleteHandler(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodDelete {
		node.sendError(w, "Only DELETE allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	bucket := r.URL.Query().Get("bucket")
	if key == "" {
		node.sendError(w, "Missing 'key'", http.StatusBadRequest)
		return
	}

	if bucket == "" {
		bucket = DefaultBucket
	}

	_, err := node.Delete(context.Background(), &rcppb.DeleteReq{Key: key, Bucket: bucket})
	if err != nil {
		log.Printf("Delete failed: %v", err)
		node.sendError(w, "Delete operation failed", http.StatusInternalServerError)
		return
	}

	node.sendJSON(w, DeleteResponse{Success: true, Message: "Deleted successfully"})
}

func (node *Node) sendJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (node *Node) sendError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message, Success: false})
}
