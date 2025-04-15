package node

import (
	// "encoding/json"
	// "log"
	// "net/http"
	// "strconv"
)

// type SuccessResponse struct {
// 	Success bool `json:"success"`
// }

// type GetKVResponse struct {
// 	Value string `json:"value"`
// }

// func (node *Node) handler(w http.ResponseWriter, r *http.Request) {
// 	log.Println("Received HTTP request (store)")

// 	if r.Method != http.MethodPost {
// 		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	key := r.URL.Query().Get("key")
// 	value := r.URL.Query().Get("value")

// 	if key == "" || value == "" {
// 		http.Error(w, "Missing 'key' or 'value'", http.StatusBadGateway)
// 		return
// 	}

// 	log.Printf("Received data: Key=%s, Value=%s", key, value)

// 	if !node.isLeader {
// 		go node.forwardToLeader(key, value)
// 	} else {
// 		node.logBufferChan <- &rcppb.LogEntry{
// 			LogType: "store",
// 			Key:     key,
// 			Value:   value,
// 		}
// 	}

// 	respData := SuccessResponse{
// 		Success: true,
// 	}

// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(respData)
// }

// func (node *Node) getHandler(w http.ResponseWriter, r *http.Request) {
// 	key := r.URL.Query().Get("key")
// 	w.Header().Set("Content-Type", "application/json")
// 	value, err := node.db.GetKV(key)

// 	if err != nil {
// 		log.Printf("%v\n", err)
// 		respData := GetKVResponse{
// 			Value: "",
// 		}
// 		json.NewEncoder(w).Encode(respData)
// 		return
// 	}
// 	respData := GetKVResponse{
// 		Value: value,
// 	}
// 	json.NewEncoder(w).Encode(respData)
// }

// func (node *Node) killHandler(w http.ResponseWriter, r *http.Request) {
// 	node.Live = false
// 	response := SuccessResponse{
// 		Success: true,
// 	}
// 	json.NewEncoder(w).Encode(response)
// }

// func (node *Node) reviveHandler(w http.ResponseWriter, r *http.Request) {
// 	node.Live = true
// 	response := SuccessResponse{
// 		Success: true,
// 	}
// 	json.NewEncoder(w).Encode(response)
// }


// func (node *Node) partitionHandler(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodPost {
// 		http.Error(w, "Only POST method allowed", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	nodesList := r.URL.Query().Get("nodes")
// 	if nodesList == "" {
// 		http.Error(w, "Missing required 'nodes' query parameter", http.StatusBadRequest)
// 		return
// 	}

// 	// Split and clean the nodes list
// 	nodes := strings.Split(nodesList, ",")
// 	if len(nodes) == 0 {
// 		http.Error(w, "Empty nodes list not allowed", http.StatusBadRequest)
// 		return
// 	}

// 	// Clean up whitespace and validate individual nodes
// 	cleanedNodes := make([]string, 0, len(nodes))
// 	for _, n := range nodes {
// 		trimmed := strings.TrimSpace(n)
// 		if trimmed == "" {
// 			http.Error(w, "Empty node names not allowed", http.StatusBadRequest)
// 			return
// 		}
// 		cleanedNodes = append(cleanedNodes, trimmed)
// 	}

// 	node.reachableSetLock.Lock()
// 	node.reachableNodes = make(map[string]struct{})
// 	for _, nodeId := range cleanedNodes {
// 		node.reachableNodes[nodeId] = struct{}{}
// 	}
// 	node.reachableSetLock.Unlock()
	
// 	respData := SuccessResponse{
// 		Success: true,
// 	}

// 	w.Header().Set("Content-Type", "application/json")
// 	w.WriteHeader(http.StatusOK)
// 	if err := json.NewEncoder(w).Encode(respData); err != nil {
// 		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
// 		return
// 	}
// }

// func (node *Node) delayHandler(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodPost {
// 		http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	to := r.URL.Query().Get("to")
// 	delay := r.URL.Query().Get("delay")

// 	if delay == "" || to == "" {
// 		http.Error(w, "Missing 'key' or 'value'", http.StatusBadGateway)
// 		return
// 	}

// 	log.Printf("Setting delay: To=%s, Delay=%s", to, delay)
// 	var respData SuccessResponse
// 	delayInt, err := strconv.ParseInt(delay, 10, 32)
// 	if err != nil {
// 		log.Printf("Error converting delay to int: %v", err)
// 		respData = SuccessResponse{
// 			Success: false,
// 		}
// 	} else {
// 		node.delays.Store(to, int(delayInt))
// 		respData = SuccessResponse{
// 			Success: true,
// 		}
// 	}
	
// 	w.Header().Set("Content-Type", "application/json")
// 	json.NewEncoder(w).Encode(respData)
// }

// func (node *Node) startHttpServer() {
// 	log.Printf("Starting HTTP server on port %s\n", node.HttpPort)
	// http.HandleFunc("/put", node.handler)
	// http.HandleFunc("/get", node.getHandler)
	// http.HandleFunc("/kill", node.killHandler)
	// http.HandleFunc("/revive", node.reviveHandler)
	// http.HandleFunc("/partition", node.partitionHandler)
	// http.HandleFunc("/delay", node.delayHandler)
	// err := http.ListenAndServe(node.HttpPort, nil)
	// if err != nil {
	// 	log.Printf("Failed to start HTTP server: %v", err)
	// }
// }
