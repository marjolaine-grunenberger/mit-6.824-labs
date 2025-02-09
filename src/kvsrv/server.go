package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	store map[string]string	// key value store
	clientRequests map[int64]int64 	// maps clinet id to the last seen request id
	requestResults map[int64]string	// maps request id to the result of the request,
	// if the value is the request id, then the request has not been processed yet
}

// Acknowledges the client request, if a request is acknowledged,
// then the server will not process the request again, references to
// the client and the request are deleted from clientRequests and requestResults
// to free up memory
func (kv *KVServer) Ack(args *AckArgs, reply *AckReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.clientRequests, args.ClientId)
	delete(kv.requestResults, args.RequestId)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// Returns value for key, or "" if key doesn't exist
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.store[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// If RequestId is not in clientRequests, then update the value and add requestId to clientRequests
	// otherwise, return the value that was returned for the same requestId
	if prevRequestId, ok := kv.clientRequests[args.ClientId]; !ok || prevRequestId != args.RequestId {
		reply.Value = args.Value
		kv.store[args.Key] = args.Value
		if ok {
			prevRequestId := kv.clientRequests[args.ClientId]
			delete(kv.requestResults, prevRequestId)
		}
		kv.clientRequests[args.ClientId] = args.RequestId
		kv.requestResults[args.RequestId] = reply.Value
	} else {
		reply.Value = kv.requestResults[args.RequestId]
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// If clientRequests[clientId] does not exist or is different than requestId, then update the value and
	// update clientRequests[clientId] to requestId
	if prevRequestId, ok := kv.clientRequests[args.ClientId]; !ok || prevRequestId != args.RequestId {
		reply.Value = kv.store[args.Key] // Append returns the old value for key, or "" if key doesn't exist
		kv.store[args.Key] = kv.store[args.Key] + args.Value
		if ok {
			prevRequestId := kv.clientRequests[args.ClientId]
			delete(kv.requestResults, prevRequestId)
		}
		kv.clientRequests[args.ClientId] = args.RequestId
		kv.requestResults[args.RequestId] = reply.Value
	} else {
		reply.Value = kv.requestResults[args.RequestId]
	}
}

func StartKVServer() *KVServer {
	return &KVServer{ 
		mu: sync.Mutex{},
		store: make(map[string]string),
		clientRequests: make(map[int64]int64),
		requestResults: make(map[int64]string),
	}
}
