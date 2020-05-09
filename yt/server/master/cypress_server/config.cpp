#include "config.h"

#include <yt/ytlib/journal_client/helpers.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

TCypressManagerConfig::TCypressManagerConfig()
{
    RegisterParameter("default_file_replication_factor", DefaultFileReplicationFactor)
        .Default(3)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    RegisterParameter("default_table_replication_factor", DefaultTableReplicationFactor)
        .Default(3)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    RegisterParameter("default_journal_erasure_codec", DefaultJournalErasureCodec)
        .Default(NErasure::ECodec::None);
    RegisterParameter("default_journal_replication_factor", DefaultJournalReplicationFactor)
        .Default(3)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    RegisterParameter("default_journal_read_quorum", DefaultJournalReadQuorum)
        .Default(2)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);
    RegisterParameter("default_journal_write_quorum", DefaultJournalWriteQuorum)
        .Default(2)
        .InRange(NChunkClient::MinReplicationFactor, NChunkClient::MaxReplicationFactor);

    RegisterPostprocessor([&] () {
        NJournalClient::ValidateJournalAttributes(
            DefaultJournalErasureCodec,
            DefaultJournalReplicationFactor,
            DefaultJournalReadQuorum,
            DefaultJournalWriteQuorum);
    });
}

////////////////////////////////////////////////////////////////////////////////

TDynamicCypressManagerConfig::TDynamicCypressManagerConfig()
{
    RegisterParameter("statistics_flush_period", StatisticsFlushPeriod)
        .GreaterThan(TDuration())
        .Default(TDuration::Seconds(1));
    RegisterParameter("max_node_child_count", MaxNodeChildCount)
        .GreaterThan(20)
        .Default(50000);
    RegisterParameter("max_string_node_length", MaxStringNodeLength)
        .GreaterThan(256)
        .Default(65536);
    RegisterParameter("max_attribute_size", MaxAttributeSize)
        .GreaterThan(256)
        .Default(16_MB);
    RegisterParameter("max_map_node_key_length", MaxMapNodeKeyLength)
        .GreaterThan(256)
        .Default(4096);

    RegisterParameter("expiration_check_period", ExpirationCheckPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("max_expired_nodes_removals_per_commit", MaxExpiredNodesRemovalsPerCommit)
        .Default(1000);
    RegisterParameter("expiration_backoff_time", ExpirationBackoffTime)
        .Default(TDuration::Seconds(10));

    RegisterParameter("tree_serialization_codec", TreeSerializationCodec)
        .Default(NCompression::ECodec::Lz4);

    RegisterParameter("forbid_set_command", ForbidSetCommand)
        .Default(false);
    RegisterParameter("enable_unlock_command", EnableUnlockCommand)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

