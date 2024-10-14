### How it works

{{product-name}} supports master and tablet [transactions](../../../user-guide/storage/transactions.md). Master transactions use a pessimistic locking model. This means that a lock on a Cypress node is acquired synchronously, and the system checks for potential conflicts.

Using the pessimistic model, you can construct a simple emulation of a distributed lock:

1. Create a node in Cypress outside of transactions. This node will respond to the lock. The node can be of any type, for example, `map_node`.
2. In the program, create a transaction where you want to acquire a lock on the given node. An attempt to acquire an exclusive lock on the node is under this transaction.
3. If the lock is acquired, then you can execute the code that this lock protects. And it's important that you ping the transaction. If the ping call fails or the transaction is canceled, then the execution is no longer exclusive. In this case, you have to stop the process or take other actions.
4. If the lock **isn't** acquired, repeat the transaction creation and lock acquisition.

As with any distributed lock system, you can't fully guarantee that a process under a lock is exclusive when it makes changes to a third-party system that is not integrated with {{product-name}} transactions. For example, between consecutive ping calls, it might happen that a transaction is interrupted and a parallel process that acquires a lock on the same node starts.

When you make changes on the cluster where the lock is acquired, you should use the `prerequisite_transactions` option in requests to the cluster. In this option, you should specify the transaction under which the lock was acquired. Thus, {{product-name}} ensures that the request is executed only if the transaction is alive and the lock is acquired.

### How to use

To run a process under a lock, you can use the [run-command-with-lock](../../../api/cli/commands.md#run-command-with-lock) command. Alternatively, you can define how to acquire a lock in the application code.

If the process makes changes on a cluster and needs to make them exclusively, we recommend using a lock directly on that cluster.
