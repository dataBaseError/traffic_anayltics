# Traffic Analysis

Simple golang app for simulating a large number of messages to calculate the top k.

## Run

Build and run:

```bash
$ go build top_k.go random_sample.go 
$ ./top_k
```

## Design

Messages are received from an inbound channel (j number of receiver threads). This channel then hashes the message key and fans-out the messages to 1 of n workers (1 worker for n hash buckets). The running total is then kept per thread and thus eliminate the need for locking. Therefore n buckets means n worker threads.

The total for each key is maintained thread-local and an update is sent along a channel to calculate top-k. m number of threads are processing the top-k updates. A rough check is made if the update is relevant, if not then the update is dropped. If it is relevant, the lock is grabbed and exact check is made if the update is relevant. If not relevant the update is dropped and the lock is released. The top-k list is updated appropriately.

A binary search is used to find the correct location to update and `copy` is used to provide a quick update.

## Additional Work

De-duping wasn't fully implemented. Since the data was just generate it would be difficult to assume hard-rules on it. E.g. message timestamps must be increasing. If this were the case one could easily store a thread-local map of each key's last timestamp and drop messages received before it. Updating the key for each "new" message.

Alternatively a simple solution of using Redis would be possible but detract from performance.\

Persistence was also a consideration generally ignored in this example. However it would be rather easy to store the state of each thread-local map or other state objects. It would be necessary to implement the de-duping of course. And finally a means to load the stored data back.
