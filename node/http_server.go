package node

import (
	"encoding/json"
	"log"
	"net/http"
	"rcp/rcppb"
)

type StoreResponse struct {
	Success bool `json:"success"`
}

type GetKVResponse struct {
	Value string `json:"value"`
}

func (node *Node) handler(w http.ResponseWriter, r *http.Request) {
	log.Println("Received HTTP request (store)")

	if r.Method != http.MethodPost {
		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	if key == "" || value == "" {
		http.Error(w, "Missing 'key' or 'value'", http.StatusBadGateway)
		return
	}

	log.Printf("Received data: Key=%s, Value=%s", key, value)
	node.logBufferChan <- &rcppb.LogEntry{
		LogType: "store",
		Key:   key,
		Value: value,
	}

	respData := StoreResponse{
		Success: true,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(respData)
}

func (node *Node) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	w.Header().Set("Content-Type", "application/json")
	value, err := node.db.GetKV(key)

	if err != nil {
		log.Printf("%v\n", err)
		respData := GetKVResponse {
			Value: "",
		}
		json.NewEncoder(w).Encode(respData)
		return
	}
	respData := GetKVResponse {
		Value: value,
	}
	json.NewEncoder(w).Encode(respData)
}

func (node *Node) killHandler(w http.ResponseWriter, r *http.Request) {
	node.Live = false
	response := StoreResponse{
		Success: true,
	}
	json.NewEncoder(w).Encode(response)
}


func (node *Node) reviveHandler(w http.ResponseWriter, r *http.Request) {
	node.Live = true
	response := StoreResponse{
		Success: true,
	}
	json.NewEncoder(w).Encode(response)
}

func (node *Node) startHttpServer() {
	log.Printf("Starting HTTP server on port %s\n", node.HttpPort)
	http.HandleFunc("/put", node.handler)
	http.HandleFunc("/get", node.getHandler)
	http.HandleFunc("/kill", node.killHandler)
	http.HandleFunc("/revive", node.reviveHandler)
	err := http.ListenAndServe(node.HttpPort, nil)
	if err != nil {
		log.Printf("Failed to start HTTP server: %v", err)
	}
}
