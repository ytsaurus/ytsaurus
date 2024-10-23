# Extending master servers

Adding new master cells always requires complete cluster downtime. In simplified form, cluster extension is a simultaneous update of the static config of all cluster components with some additional actions. Below is a detailed action plan. We strongly advise you read it to the end before proceeding.

## Preparation

1. You need to prepare a new static config and add information about the new cells to it.
2. You can prepare @cluster_connection with the new cells in advance (to learn more about @cluster_connection, see item 14).

{% note warning "Important" %}

Cell tags / cell IDs must be unique and ideally should follow a predictable pattern. If you have a multi-cluster installation, cell tags / cell IDs must be unique among all master cells of all your installation clusters.

{% endnote %}

## Detailed plan

### 1. Shut down schedulers and controller agents
These components must be shut down first, because they can abort user operations if a large number of nodes leave (which entails a lack of resources).

### 2. Disable chunk refresh and chunk requisition update
```bash
yt set //sys/@config/chunk_manager/enable_chunk_refresh %false
yt set //sys/@config/chunk_manager/enable_chunk_requisition_update %false
```

Master servers have background chunk replication. In the next step, we are going to shut down all nodes, causing the chunks to inevitably lose their replicas: this may create a lot of needless load (although master servers do have safeguards that trigger when too many replicas are lost, they'd still be doing needless work before these safeguards activate). To prevent this, you can toggle the described flags in advance.

### 3. Shut down all nodes
(You can monitor the state of nodes at //sys/nodes/@count and //sys/nodes/@online_node_count).

Make sure that all cluster nodes are offline (you can check the state using //sys/cluster_nodes/{node_address}/@state). Master servers have a protection that prevents the registration of new cells if there is at least one non-offline node.

{% note warning "Important" %}

If the cluster contains secondary cells, the nodes may be in mixed state for a while. In this case, you also need to wait for all nodes to go offline.

{% endnote %}

### 4. Shut down all proxies

### 5. Shut down all the remaining services **except** master servers
(Most likely, these are clock servers, master caches, timestamp providers, and discovery servers. If there are other services, disable them at this step as well).

{% note warning "Note" %}

During extension, you do not have to disable services that do not rely on master servers in their operations. Examples of such services are clock servers, discovery servers, and possibly other services that do not have master server addresses in their static config.

If for some reason you need these services to stay alive, you do not have to shut them down at this step. Still, we strongly suggest double checking: failure to disable a service that DOES have master servers in its static config (which then will not be updated) may result in the cluster breaking in an arbitrary way.

{% endnote %}

### 6. Create a read-only snapshot on the master servers
```bash
yt-admin build-master-snapshots —read-only —wait-for-snapshot-completion
```

Read-only is a special cluster state in which master servers cannot accept, schedule, or apply mutations. Creating a snapshot in this mode is a standard technique used for major updates. It helps ensure that the updated master servers will recover only from the snapshot, without applying a changelog over the snapshot (because the changelog will be empty after the snapshot).

{% note warning "Attention" %}

This step is very important: not following it may result in loss of data.

{% endnote %}

You can verify that the master servers are in read-only mode and have created a snapshot by leader logs:
"Read-only mode enabled" and "Distributed snapshot creation finished".

### 7. Shut down the master servers

### 8. Deploy the master servers, clock servers, master caches, timestamp providers, and discovery servers

{% note warning "Important" %}

If any services other than the ones listed above were disabled at step 5, **do not** deploy them yet!

{% endnote %}

### 9. Exit read-only mode
```bash
yt-admin master-exit-read-only
```

### 10. Wait for the registration of new cells and replication of global objects
Use the master server logs to verify that the new cells have been successfully registered and global master objects have been replicated on them.

Search for the registration success message in the logs of the primary cell: "Master cell registered".
Object replication messages can be found in the logs of the newly added master cells: "Foreign object created" (you may have to wait for a couple of minutes for the messages to start appearing). Note that there may be many objects on the cluster, so you want to wait for the global object replication to complete (wait for the flow of "Foreign object created" messages to stop).

### 11. Wait for the master servers to return to their normal operation mode
Make sure that all master servers, including other secondary cells if any, are in normal operation mode.

You can search for the "Leader active"/"Follower active" message in the logs.

### 12. Assign roles to the new cells

You need secondary master cells to shard the load, and there are different methods to do that. Here you need to choose what kind of work the new cell will be doing.

Current types of roles:
*`chunk_host`: Sharding by chunks, assigns the work with table chunks to the cell. The most popular and once default role. After this role is specified, new chunks will automatically start populating the cell.
*`cypress_node_host`: Sharding of the metainformation tree, assigns Cypress subtrees to this cell (this requires manual actions; you can do that with this role, but there will be no automation). This article does not cover the procedure for placing subtrees on a separate cell.
*`transaction_coordinator`: Sharding of the work with transactions.

If you do not know what to do, the best way is probably to make the new cells host chunks (chunk_host). The other two roles are needed only for very large clusters.

You can assign the new roles at `//sys/@config/multicell_manager/cell_descriptors`:

```bash
//sys/@config/multicell_manager/cell_descriptors/{cell_tag}/roles
```

Remember that proxy servers are not up at this point in time, and any access to Cypress is possible only through a correctly configured native client. Such a client can usually be found in the master's container.

You can assign the roles later, but do not forget to do that; if you do, the new cells will not be doing anything useful.

### 13. Deploy nodes (to the version with the new config)

{% note warning "Important" %}

This step is a point of no return. In theory, you can still roll back the addition of cells at any step before this one. After this step, reverting the addition of new cells will inevitably result in loss of data.

{% endnote %}

### 14. Write new cells to cluster connection
Cluster connection resides in Cypress at `//sys/@cluster_connection/secondary_masters`.
You need to write cells in the following format:
```bash
[
        {
            "addresses" = [
                "some_master_address:9010";
            ];
            "cell_id" = "1-2-3-4";
        };
];
```

{% note warning "Attention" %}

The new cells must be ADDED. If the cluster already contains secondary master cells, the new ones should simply be appended to the end of the list. We recommend saving the old cluster_connection somewhere before modifying it.

{% endnote %}

### 15. Enable chunk refresh and chunk requisition update
If you did not disable them at step 2, skip this step.
```bash
yt set //sys/@config/chunk_manager/enable_chunk_refresh %true
yt set //sys/@config/chunk_manager/enable_chunk_requisition_update %true
```
### 16. Wait until all nodes are registered and the chunk refresh process completes successfully
Depending on the number of nodes and chunks on the cluster, this step may take a significant amount of time.

### 17. Deploy other components
Deploy proxies, schedulers, controller agents, and other components (if any are remaining).
Components must be deployed to the version with the updated config.

### 18. Wait for the cluster to return to its normal operation mode
