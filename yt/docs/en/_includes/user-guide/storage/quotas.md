# Quotas

This section describes resource quotas in the {{product-name}} system.

## Resource types { #resources }

Most {{product-name}} clusters are shared. Cluster resources are shared out to all projects that require data processing and storage. These quotas are displayed but not enabled.
The following types of resource quotas exist:

* Storage:
   * HDD (SATA) disk quota not accounting for compression or replication.
   * SATA SSD/NVMe SSD disk quota not accounting for compression or replication.
   * Quota on the number of nodes in the [metadata tree](../../../user-guide/storage/cypress.md) (tables, files, locks). Not booked separately in the annual planning process but provided upon request and approval. There is a limit on the number of tree nodes that applies to an entire cluster.
   * Quota on chunks that all the stored data are split into. Issued in proportion to disk quotas based on 512 MB of space per chunk for static tables and 64 MB per chunk for dynamic tables plus a margin of 200,000 chunks or as may be agreed on a per-project basis.
   * Quota on the master server RAM used by the nodes in the metadata tree. Not requested as a separate resource but rather issued subject to agreement. Takes into account chunk and node metadata, node attributes, [dynamic table](../../../user-guide/dynamic-tables/overview.md) pivot keys as well as other data. Rule-of-thumb estimate: 100 to 200 bytes per node and the same amount per chunk. The above values may vary greatly depending on attribute and node name lengths, the lengths of the values in chunk key columns, and so on.
   * Quota on the number of dynamic table [tablets (shards)](../../../user-guide/dynamic-tables/overview.md#tablets). Not requested separately but issued upon request. The quota is allocated for the [tablet_cell_bundle](../../../user-guide/dynamic-tables/concepts.md#tablet_cell_bundles) object.
* Processing:
   * Guaranteed number of HyperThreading cores for MapReduce operations.
   * Guaranteed amount of RAM for MapReduce operations.
   * Limit on the number of running MapReduce operations and the total number of operations, including those queued to run.
   * Dedicated computational resources for querying [dynamic tables](../../../user-guide/dynamic-tables/overview.md) (write/select/lookup): CPU, RAM. This includes the quota on the amount of data in dynamic tables in memory (tablet_static_memory). Quotas are allocated for the [tablet_cell_bundle](../../../user-guide/dynamic-tables/concepts.md#tablet_cell_bundles) object.

## Data storage { #account_quotas }

Most types of data storage quotas are linked to a project **account**. An account is a service object in Cypress. Each Cypress node has a readable/writable `account` attribute storing the name of the account the node in question is linked to. In the case of files and tables, assigning an account causes its disk quota to be consumed. This takes into account the space physically allocated on the cluster nodes for data storage, so it depends on the replication degree (the `replication_factor` attribute), erasure coding method (the `erasure_codec` attribute), and the compression utilized (the `compression_codec` attribute).

If the user exceeds the account quota for certain resource types, the system starts denying the account further access to the exceeded resource type. This does not mean that an account never exceeds its limits when used. For example, the limits may have been forcibly set below usage. Besides that, disk and chunk quota consumption tracking works asynchronously, which can also result in exceeding allocated limits.

For more information, see [Accounts](../../../user-guide/storage/accounts.md).

### Media { #medium }

Different media types (HDD, SDD, RAM) are logically combined in special entities referred to as [media](../../../user-guide/storage/media.md). Media types differ by the amount of available space and read/write performance. The higher the performance of a medium, the smaller the medium is. Media divide the entire set of chunk locations into non-intersecting subsets. At the same time, a single cluster node may have locations on different media. Each such subset does its own chunk balancing and handles its own quota. An account's disk quota is issued for a specific medium. A user may select the medium to use to store different types of data. {{product-name}} clusters host the following media types:

- default: integrates the set of a cluster's HDDs.
- ssd_blobs: set of cluster's SSDs.
- ssd_journals: set of SSDs for dynamic table log storage.
- in_memory: dedicated space allocated in cluster node RAM.

There is a special page in the {{product-name}} web interface to view account quotas.

For more information on media, see the [Media](../../../user-guide/storage/media.md) section.

### Dynamic table resources { #dyntable_resources }

Quotas on the number of tablets and the amount of tablet static memory are linked to tablet cell bundles. Bundle quotas display in a separate web interface section. Monitoring settings and viewing account and bundle quota histories are described in the Quota monitoring section.

## Data processing { #data_processing }

Data processing computational resources are linked to [pools](../../../user-guide/data-processing/scheduler/scheduler-and-pools.md) (except tablet_cell_bundle).
You can view computational pool settings, including guarantees and operation number limits on the **Scheduling** page (**Details** tab, **CPU** section, `Strong guar` column).

{% if audience == "public" %}

If a project uses dynamic tables for production, you need to have a separate [tablet cell bundle](../../../user-guide/dynamic-tables/concepts.md#tablet_cell_bundles) to isolate from other users. A tablet cell bundle is a group of containers (instances) that processes queries to a project's dynamic tables.

{% endif %}

{% note info "Note" %}

Dynamic tables require a disk quota to write journals (medium ssd_journals) and snapshots (medium default){% if audience == "internal" %}, which is allocated automatically{% endif %}. The required quotas are a quota on file system objects (node_count) and a quota on the number of tablets (table shards, or tablet_count).

{% endnote %}

{% if audience == "internal" %}

{% note warning "Attention" %}

Users are responsible for monitoring resource allocation quotas and ordering upgrades in a timely manner when necessary.

{% endnote %}

{% endif %}
