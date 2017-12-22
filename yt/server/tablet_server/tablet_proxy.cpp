#include "tablet_proxy.h"
#include "private.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/chunk_server/chunk_list.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/table_server/table_node.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTabletServer {

using namespace NYson;
using namespace NYTree;
using namespace NObjectServer;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TTabletProxy
    : public TNonversionedObjectProxyBase<TTablet>
{
public:
    TTabletProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTablet* tablet)
        : TBase(bootstrap, metadata, tablet)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTablet> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* tablet = GetThisImpl();
        const auto* table = tablet->GetTable();

        descriptors->push_back("state");
        descriptors->push_back("statistics");
        descriptors->push_back(TAttributeDescriptor("table_path")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("trimmed_row_count")
            .SetPresent(!table->IsPhysicallySorted()));
        descriptors->push_back(TAttributeDescriptor("flushed_row_count")
            .SetPresent(!table->IsPhysicallySorted()));
        descriptors->push_back("last_commit_timestamp");
        descriptors->push_back("last_write_timestamp");
        descriptors->push_back(TAttributeDescriptor("performance_counters")
            .SetPresent(tablet->GetCell()));
        descriptors->push_back(TAttributeDescriptor("mount_revision")
            .SetPresent(tablet->GetCell()));
        descriptors->push_back(TAttributeDescriptor("stores_update_prepared")
            .SetPresent(tablet->GetStoresUpdatePreparedTransaction() != nullptr));
        descriptors->push_back("index");
        descriptors->push_back("table_id");
        descriptors->push_back(TAttributeDescriptor("pivot_key")
            .SetPresent(table->IsPhysicallySorted()));
        descriptors->push_back("chunk_list_id");
        descriptors->push_back("in_memory_mode");
        descriptors->push_back(TAttributeDescriptor("cell_id")
            .SetPresent(tablet->GetCell()));
        descriptors->push_back(TAttributeDescriptor("action_id")
            .SetPresent(tablet->GetAction()));
        descriptors->push_back("retained_timestamp");
        descriptors->push_back("unflushed_timestamp");
    }

    virtual bool GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer) override
    {
        const auto* tablet = GetThisImpl();
        const auto* table = tablet->GetTable();
        const auto* chunkList = tablet->GetChunkList();

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        if (key == "state") {
            BuildYsonFluently(consumer)
                .Value(tablet->GetState());
            return true;
        }

        if (key == "statistics") {
            BuildYsonFluently(consumer)
                .Value(New<TSerializableTabletStatistics>(
                    tabletManager->GetTabletStatistics(tablet),
                    chunkManager));
            return true;
        }

        if (key == "table_path" && IsObjectAlive(tablet->GetTable())) {
            BuildYsonFluently(consumer)
                .Value(cypressManager->GetNodePath(
                    tablet->GetTable()->GetTrunkNode(),
                    nullptr));
            return true;
        }

        if (key == "trimmed_row_count") {
            BuildYsonFluently(consumer)
                .Value(tablet->GetTrimmedRowCount());
            return true;
        }

        if (key == "flushed_row_count") {
            BuildYsonFluently(consumer)
                .Value(chunkList->Statistics().LogicalRowCount);
            return true;
        }

        if (key == "last_commit_timestamp") {
            BuildYsonFluently(consumer)
                .Value(tablet->NodeStatistics().last_commit_timestamp());
            return true;
        }

        if (key == "last_write_timestamp") {
            BuildYsonFluently(consumer)
                .Value(tablet->NodeStatistics().last_write_timestamp());
            return true;
        }

        if (tablet->GetCell()) {
            if (key == "performance_counters") {
                BuildYsonFluently(consumer)
                    .Value(tablet->PerformanceCounters());
                return true;
            }

            if (key == "mount_revision") {
                BuildYsonFluently(consumer)
                    .Value(tablet->GetMountRevision());
                return true;
            }
        }

        if (key == "stores_update_prepared_transaction_id" && tablet->GetStoresUpdatePreparedTransaction()) {
            BuildYsonFluently(consumer)
                .Value(tablet->GetStoresUpdatePreparedTransaction()->GetId());
            return true;
        }

        if (key == "index") {
            BuildYsonFluently(consumer)
                .Value(tablet->GetIndex());
            return true;
        }

        if (key == "table_id") {
            BuildYsonFluently(consumer)
                .Value(table->GetId());
            return true;
        }

        if (key == "pivot_key" && table->IsPhysicallySorted()) {
            BuildYsonFluently(consumer)
                .Value(tablet->GetPivotKey());
            return true;
        }

        if (key == "chunk_list_id") {
            BuildYsonFluently(consumer)
                .Value(tablet->GetChunkList()->GetId());
            return true;
        }

        if (key == "in_memory_mode") {
            BuildYsonFluently(consumer)
                .Value(tablet->GetInMemoryMode());
            return true;
        }

        if (key == "cell_id" && tablet->GetCell()) {
            BuildYsonFluently(consumer)
                .Value(tablet->GetCell()->GetId());
            return true;
        }

        if (key == "action_id" && tablet->GetAction()) {
            BuildYsonFluently(consumer)
                .Value(tablet->GetAction()->GetId());
            return true;
        }

        if (key == "retained_timestamp") {
            BuildYsonFluently(consumer)
                .Value(tablet->GetRetainedTimestamp());
            return true;
        }

        if (key == "unflushed_timestamp") {
            BuildYsonFluently(consumer)
                .Value(static_cast<TTimestamp>(tablet->NodeStatistics().unflushed_timestamp()));
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

IObjectProxyPtr CreateTabletProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTablet* tablet)
{
    return New<TTabletProxy>(bootstrap, metadata, tablet);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

