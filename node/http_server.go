package node

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"rcp/constants"
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
	Error string `json:"error"`
}

type CauseFailureResponse struct {
	Success bool `json:"success"`
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

	// log.Printf("Got put at %v", time.Since(node.beginTime))

	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	bucket := r.URL.Query().Get("bucket")

	if key == "" || value == "" {
		node.sendError(w, "Missing 'key' or 'value'", http.StatusBadRequest)
		return
	}

	if bucket == "" {
		bucket = constants.DefaultBucket
	}

	data, err := node.Store(key, bucket, value)

	if err != nil {
		log.Printf("Store failed: %v", err)

		if errors.Is(err, ErrNotLeader) {
			node.sendError(w, data, http.StatusTemporaryRedirect)
			return
		}

		if errors.Is(err, ErrTimeOut) {
			node.sendError(w, "Timeout waiting for consensus", http.StatusGatewayTimeout)
			return
		}

		node.sendError(w, "Store operation failed", http.StatusInternalServerError)
		return
	}

	node.sendJSON(w, StoreResponse{Success: true})
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

	if bucket == "" {
		bucket = constants.DefaultBucket
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

	if bucket == "" {
		bucket = constants.DefaultBucket
	}

	data, err := node.Delete(key, bucket)

	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			node.sendError(w, data, http.StatusTemporaryRedirect)
			return
		}

		if errors.Is(err, ErrTimeOut) {
			node.sendError(w, "Timeout waiting for consensus", http.StatusGatewayTimeout)
			return
		}

		log.Printf("Delete failed: %v", err)
		node.sendError(w, "Delete operation failed", http.StatusInternalServerError)
		return
	}

	node.sendJSON(w, DeleteResponse{Success: true})
}

func (node *Node) sendJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (node *Node) sendError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{Error: message})
}

func (node *Node) causeFailureHandler(w http.ResponseWriter, r *http.Request) {
	failureType := r.URL.Query().Get("type")
	log.Printf("Got cause-failure of type %s", failureType)

	node.mutex.Lock()
	defer node.mutex.Unlock()

	switch failureType {
	case "revive":
		if node.Live {
			node.sendJSON(w, CauseFailureResponse{Success: false})
		} else {
			node.Live = true
			node.sendJSON(w, CauseFailureResponse{Success: true})
		}

	case "leader":
		if node.Live {
			if node.isLeader {
				node.Live = false
				node.sendJSON(w, CauseFailureResponse{Success: true})
			} else {
				// Node is not the leader, redirect to the leader
				node.sendError(w, node.votedFor, http.StatusTemporaryRedirect)
			}
		} else {
			// Node is not alive, the node doesn't know the correct leader
			node.sendJSON(w, CauseFailureResponse{Success: false})
		}

	case "non-leader":
		if node.isLeader {
			node.sendJSON(w, CauseFailureResponse{Success: false})
		} else {
			if node.Live {
				node.Live = false
				node.sendJSON(w, CauseFailureResponse{Success: true})
			} else {
				node.sendJSON(w, CauseFailureResponse{Success: false})
			}
		}

	case "random":
		if node.Live {
			node.Live = false
			node.sendJSON(w, CauseFailureResponse{Success: true})
		} else {
			node.sendJSON(w, CauseFailureResponse{Success: false})
		}

	default:
		node.sendError(w, "invalid failure type", http.StatusBadRequest)
		return
	}
}

// func (node *Node) maybeReviveDeadNode(w http.ResponseWriter) {
// 	nodeToRevive := ""

// 	for nodeId := range node.failedSet {
// 		nodeToRevive = nodeId
// 		break
// 	}

// 	if !node.Live {
// 		nodeToRevive = node.Id
// 	}

// 	switch nodeToRevive {
// 	case "":
// 		node.sendError(w, "No dead nodes", http.StatusBadRequest)
// 		return
// 	case node.Id:
// 		_, err := node.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: true})
// 		if err != nil {
// 			node.sendError(w, err.Error(), http.StatusInternalServerError)
// 			return
// 		}
// 	default:
// 		RPCClient, ok := node.ClientMap[nodeToRevive]
// 		if !ok {
// 			node.sendError(w, fmt.Sprintf("Invalid server or no gRPC client for '%s'", nodeToRevive), http.StatusInternalServerError)
// 			return
// 		}
// 		RPCClient.SetStatus(context.Background(), &wrapperspb.BoolValue{Value: true})
// 	}

// 	node.sendJSON(w, CauseFailureResponse{Success: true, NodeRevived: nodeToRevive})
// }
