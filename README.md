# dz2

You simulate a database with replicas. The client sends data to the master server, from the master the data is replicated to other nodes. Reading is distributed evenly across all replicas (i.e. the client's read request is served not by the master, but by some replica). If the master is lost, the replicas must vote and choose a new master among the living nodes using the consensus protocol (Raft).

If the master comes back to life and has some unsynchronized data, then it must be processed in a reasonable way, and the former master must become one of the replicas.
A separate point is the implementation of a linearizable atomic CAS

The system must perform CRUD operations - create/read/update/delete
When reading, there is no need to pump data from the replica through the master, the data must go from the replica to the client. For this, the master can respond, for example, with 302 Found and provide a Location header with the replica address
Consider the semantics of HTTP methods - PUT is idempotent (and requires a resource ID in the request), POST is non-idempotent, PATCH allows you to update a resource partially and depends on the current state
The maximum number of replicas is fixed.t
