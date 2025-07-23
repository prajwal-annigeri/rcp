package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

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

type CauseFailureResponse struct {
	Success     bool   `json:"success"`
	NodeKilled  string `json:"node_killed"`
	NodeRevived string `json:"node_revived"`
}

func (node *Node) startHttpServer() {
	log.Printf("Starting HTTP server on port %s", node.HttpPort)
	http.HandleFunc("/put", node.putHandler)
	http.HandleFunc("/get", node.getHandler)
	http.HandleFunc("/del", node.deleteHandler)
	http.HandleFunc("/cause-failure", node.causeFailureHandler)
	err := http.ListenAndServe(node.HttpPort, nil)
	if err != nil {
		log.Printf("Failed to start HTTP server: %v", err)
	}
}

func (node *Node) putHandler(w http.ResponseWriter, r *http.Request) {
	if !node.Live {
		node.sendError(w, "not alive", http.StatusExpectationFailed)
		return
	}

	if r.Method != http.MethodPost {
		node.sendError(w, "Only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Printf("Got put at %v", time.Since(node.beginTime))

	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	bucket := r.URL.Query().Get("bucket")

	if key == "" || value == "" {
		node.sendError(w, "'key' and 'value'required", http.StatusBadRequest)
		return
	}

	// TODO: Handle redirect to leader
	err := node.Store(key, bucket, value)
	if err != nil {
		log.Printf("Store failed: %v", err)
		node.sendError(w, "Store operation failed", http.StatusInternalServerError)
		return
	}

	node.sendJSON(w, StoreResponse{Success: true, Message: "Stored successfully"})
}

func (node *Node) getHandler(w http.ResponseWriter, r *http.Request) {
	if !node.Live {
		node.sendError(w, "not alive", http.StatusExpectationFailed)
		return
	}

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

	value, err := node.Get(key, bucket)
	if err != nil {
		log.Printf("Get failed on key %s and bucket %s: %v", key, bucket, err)
		node.sendJSON(w, GetKVResponse{Value: "", Found: false})
		return
	}

	node.sendJSON(w, GetKVResponse{Value: value, Found: true})
}

func (node *Node) deleteHandler(w http.ResponseWriter, r *http.Request) {
	if !node.Live {
		node.sendError(w, "not alive", http.StatusExpectationFailed)
		return
	}

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

	// TODO: Handle redirect to leader
	err := node.Delete(key, bucket)
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

func (node *Node) causeFailureHandler(w http.ResponseWriter, r *http.Request) {
	failureType := r.URL.Query().Get("type")
	log.Printf("Got cause-failure of type %s", failureType)
	var nodeToKill string
	switch failureType {
	case "revive":
		node.maybeReviveDeadNode(w)
		return
	case "leader":
		currentLeader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			node.sendError(w, "BUG() no leader", http.StatusInternalServerError)
			return
		}
		nodeToKill = currentLeader.(string)
	case "non-leader":
		currentLeader, ok := node.votedFor.Load(node.currentTerm)
		if !ok {
			node.sendError(w, "BUG() no leader", http.StatusInternalServerError)
			return
		}

		for nodeID := range node.ClientMap {
			if nodeID != currentLeader {
				status, _ := node.serverStatusMap.Load(nodeID)
				if status.(bool) {
					nodeToKill = nodeID
					break
				}
			}
		}
	case "random":
		for nodeID := range node.ClientMap {
			status, _ := node.serverStatusMap.Load(nodeID)
			if status.(bool) {
				nodeToKill = nodeID
				break
			}
		}
	default:
		node.sendError(w, "invalid failure type", http.StatusBadRequest)
		return
	}

	if nodeToKill == node.Id {
		_, err := node.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: false})
		if err != nil {
			node.sendError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		RPCClient, ok := node.ClientMap[nodeToKill]
		if !ok {
			node.sendError(w, fmt.Sprintf("Invalid server or no gRPC client for '%s'", nodeToKill), http.StatusInternalServerError)
			return
		}
		RPCClient.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: false})
	}

	node.sendJSON(w, CauseFailureResponse{Success: true, NodeKilled: nodeToKill})
}

func (node *Node) maybeReviveDeadNode(w http.ResponseWriter) {
	nodeToRevive := ""
	node.serverStatusMap.Range(func(nodeId any, isAlive any) bool {
		if !isAlive.(bool) {
			nodeToRevive = nodeId.(string)
			return false
		}
		return true
	})

	switch nodeToRevive {
	case "":
		node.sendError(w, "No dead nodes", http.StatusBadRequest)
		return
	case node.Id:
		_, err := node.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: true})
		if err != nil {
			node.sendError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	default:
		RPCClient, ok := node.ClientMap[nodeToRevive]
		if !ok {
			node.sendError(w, fmt.Sprintf("Invalid server or no gRPC client for '%s'", nodeToRevive), http.StatusInternalServerError)
			return
		}
		RPCClient.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: true})
	}

	node.sendJSON(w, CauseFailureResponse{Success: true, NodeRevived: nodeToRevive})
}
