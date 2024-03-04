#include "config.h"

#include <yt/yt/ytlib/journal_client/helpers.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

void TCypressManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_file_replication_factor", &TThis::DefaultFileReplicationFactor)
        .Default(3)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    registrar.Parameter("default_table_replication_factor", &TThis::DefaultTableReplicationFactor)
        .Default(3)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    registrar.Parameter("default_journal_erasure_codec", &TThis::DefaultJournalErasureCodec)
        .Default(NErasure::ECodec::None);
    registrar.Parameter("default_journal_replication_factor", &TThis::DefaultJournalReplicationFactor)
        .Default(3)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    registrar.Parameter("default_journal_read_quorum", &TThis::DefaultJournalReadQuorum)
        .Default(2)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    registrar.Parameter("default_journal_write_quorum", &TThis::DefaultJournalWriteQuorum)
        .Default(2)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);

    registrar.Parameter("default_hunk_storage_erasure_codec", &TThis::DefaultHunkStorageErasureCodec)
        .Default(NErasure::ECodec::None);
    registrar.Parameter("default_hunk_storage_replication_factor", &TThis::DefaultHunkStorageReplicationFactor)
        .Default(3)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    registrar.Parameter("default_hunk_storage_read_quorum", &TThis::DefaultHunkStorageReadQuorum)
        .Default(2)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    registrar.Parameter("default_hunk_storage_write_quorum", &TThis::DefaultHunkStorageWriteQuorum)
        .Default(2)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);

    registrar.Postprocessor([] (TThis* config) {
        NJournalClient::ValidateJournalAttributes(
            config->DefaultJournalErasureCodec,
            config->DefaultJournalReplicationFactor,
            config->DefaultJournalReadQuorum,
            config->DefaultJournalWriteQuorum);
        NJournalClient::ValidateJournalAttributes(
            config->DefaultHunkStorageErasureCodec,
            config->DefaultHunkStorageReplicationFactor,
            config->DefaultHunkStorageReadQuorum,
            config->DefaultHunkStorageWriteQuorum);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicCypressManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("statistics_flush_period", &TThis::StatisticsFlushPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_node_child_count", &TThis::MaxNodeChildCount)
        .GreaterThan(20)
        .Default(50000);
    registrar.Parameter("max_string_node_length", &TThis::MaxStringNodeLength)
        .GreaterThan(256)
        .Default(65536);
    registrar.Parameter("max_attribute_size", &TThis::MaxAttributeSize)
        .GreaterThan(256)
        .Default(16_MB);
    registrar.Parameter("max_map_node_key_length", &TThis::MaxMapNodeKeyLength)
        .GreaterThan(256)
        .Default(4096);

    registrar.Parameter("expiration_check_period", &TThis::ExpirationCheckPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_expired_nodes_removals_per_commit", &TThis::MaxExpiredNodesRemovalsPerCommit)
        .Default(1000);
    registrar.Parameter("expiration_backoff_time", &TThis::ExpirationBackoffTime)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("tree_serialization_codec", &TThis::TreeSerializationCodec)
        .Default(NCompression::ECodec::Lz4);

    registrar.Parameter("recursive_resource_usage_cache_expiration_timeout", &TThis::RecursiveResourceUsageCacheExpirationTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("default_external_cell_bias", &TThis::DefaultExternalCellBias)
        .Default(1.0)
        .InRange(0, MaxExternalCellBias);

    // COMPAT (h0pless)
    registrar.Parameter("enable_symlink_cyclicity_check", &TThis::EnableSymlinkCyclicityCheck)
        .Default(true);

    registrar.Parameter("portal_synchronization_period", &TThis::PortalSynchronizationPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("enable_portal_synchronization", &TThis::EnablePortalSynchronization)
        .Default(true)
        .DontSerializeDefault();

    registrar.Parameter("allow_cross_shard_dynamic_table_copying", &TThis::AllowCrossShardDynamicTableCopying)
        .Default(true);

    registrar.Parameter("scion_removal_period", &TThis::ScionRemovalPeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("forbid_list_node_creation", &TThis::ForbidListNodeCreation)
        .Default(false);

    registrar.Parameter("max_locks_per_transaction_subtree", &TThis::MaxLocksPerTransactionSubtree)
        .Default(100'000);

    registrar.Parameter("virtual_map_read_offload_batch_size", &TThis::VirtualMapReadOffloadBatchSize)
        .Default()
        .GreaterThanOrEqual(1)
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
