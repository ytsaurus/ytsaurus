#include "tablet_proxy.h"

#include "private.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_manager.h"
#include "tablet_proxy_base.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/static_service_dispatcher.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NYson;
using namespace NYTree;
using namespace NObjectServer;
using namespace NTransactionClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TTabletProxy
    : public TTabletProxyBase
{
public:
    using TTabletProxyBase::TTabletProxyBase;

private:
    using TBase = TTabletProxyBase;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* tablet = GetThisImpl<TTablet>();
        const auto* table = tablet->GetTable();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TrimmedRowCount)
            .SetPresent(!table->IsPhysicallySorted()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::FlushedRowCount)
            .SetPresent(!table->IsPhysicallySorted()));
        descriptors->push_back(EInternedAttributeKey::LastCommitTimestamp);
        descriptors->push_back(EInternedAttributeKey::LastWriteTimestamp);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PerformanceCounters)
            .SetOpaque(!Bootstrap_->GetConfigManager()->GetConfig()->TabletManager->AddPerfCountersToTabletsAttribute)
            .SetPresent(tablet->GetCell()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PivotKey)
            .SetPresent(table->IsPhysicallySorted()));
        descriptors->push_back(EInternedAttributeKey::RetainedTimestamp);
        descriptors->push_back(EInternedAttributeKey::UnflushedTimestamp);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UnconfirmedDynamicTableLocks)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::ReplicationErrorCount);
        descriptors->push_back(EInternedAttributeKey::BackupState);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ReplicationProgress)
            .SetPresent(!tablet->ReplicationProgress().Segments.empty()));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* tablet = GetThisImpl<TTablet>();
        const auto* chunkList = tablet->GetChunkList();
        const auto* table = tablet->GetTable();

        switch (key) {
            case EInternedAttributeKey::TrimmedRowCount:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetTrimmedRowCount());
                return true;

            case EInternedAttributeKey::FlushedRowCount:
                BuildYsonFluently(consumer)
                    .Value(chunkList->Statistics().LogicalRowCount);
                return true;

            case EInternedAttributeKey::LastCommitTimestamp:
                BuildYsonFluently(consumer)
                    .Value(tablet->NodeStatistics().last_commit_timestamp());
                return true;

            case EInternedAttributeKey::LastWriteTimestamp:
                BuildYsonFluently(consumer)
                    .Value(tablet->NodeStatistics().last_write_timestamp());
                return true;

            case EInternedAttributeKey::PerformanceCounters:
                if (!tablet->GetCell()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(tablet->PerformanceCounters());
                return true;

            case EInternedAttributeKey::PivotKey:
                if (!table->IsPhysicallySorted()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(tablet->GetPivotKey());
                return true;

            case EInternedAttributeKey::RetainedTimestamp:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetRetainedTimestamp());
                return true;

            case EInternedAttributeKey::UnflushedTimestamp:
                BuildYsonFluently(consumer)
                    .Value(static_cast<TTimestamp>(tablet->NodeStatistics().unflushed_timestamp()));
                return true;

            case EInternedAttributeKey::UnconfirmedDynamicTableLocks:
                BuildYsonFluently(consumer)
                    .Value(tablet->UnconfirmedDynamicTableLocks());
                return true;

            case EInternedAttributeKey::ReplicationErrorCount:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetReplicationErrorCount());
                return true;

            case EInternedAttributeKey::BackupState:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetBackupState());
                return true;

            case EInternedAttributeKey::ReplicationProgress:
                if (tablet->ReplicationProgress().Segments.empty()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(tablet->ReplicationProgress());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual TYPath GetOrchidPath(TTabletId tabletId) const override
    {
        return Format("tablets/%v", tabletId);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateTabletProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTablet* tablet)
{
    return New<TTabletProxy>(bootstrap, metadata, tablet);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

