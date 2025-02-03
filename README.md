## MIT 6.5840 : Distributed Systems 

Course website: https://pdos.csail.mit.edu/6.824/index.html

### [Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
The task is to implement the [MapReduce paper](http://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf), with the main difference with the original paper being that this implementation will only need to run on a single host. The intermediate files and final files are all persisted in the working directory.
`src/main/coordinator.go`, `src/main/worker.go` and `src/main/rpc.go` have been edited, all the other files are from the original [course repository](git://g.csail.mit.edu/6.5840-golabs-2025).    
Test: `bash src/main/test-mr.sh`  
`bash src/main/test-mr-many.sh $NTRIALS` to run the test $NTRIALS times and spot low probabilities bugs.

### [Lab 2: Key/Value Server](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)
The task is to implement a signle machine key value server. Full lab description can be found [here](http://nil.csail.mit.edu/6.5840/2024/labs/lab-kvsrv.html).  
`src/kvsrv/client.go`, `src/kvsrv/common.go` and `src/kvsrc/server.go` have been edited, all the other files are from the original repository. Clone it via `git clone git://g.csail.mit.edu/6.5840-golabs-2024 6.5840`.   
Test:  
`cd /src/kvsrv`  
`go test -race`  
