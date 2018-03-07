#include "tablet_proxy.h"
#include "private.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/chunk_server/chunk_list.h>

#include <yt/server/object_server/interned_attributes.h>
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

        descriptors->push_back(EInternedAttributeKey::State);
        descriptors->push_back(EInternedAttributeKey::Statistics);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TablePath)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TrimmedRowCount)
            .SetPresent(!table->IsPhysicallySorted()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::FlushedRowCount)
            .SetPresent(!table->IsPhysicallySorted()));
        descriptors->push_back(EInternedAttributeKey::LastCommitTimestamp);
        descriptors->push_back(EInternedAttributeKey::LastWriteTimestamp);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PerformanceCounters)
            .SetPresent(tablet->GetCell()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MountRevision)
            .SetPresent(tablet->GetCell()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::StoresUpdatePrepared)
            .SetPresent(tablet->GetStoresUpdatePreparedTransaction() != nullptr));
        descriptors->push_back(EInternedAttributeKey::Index);
        descriptors->push_back(EInternedAttributeKey::TableId);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PivotKey)
            .SetPresent(table->IsPhysicallySorted()));
        descriptors->push_back(EInternedAttributeKey::ChunkListId);
        descriptors->push_back(EInternedAttributeKey::InMemoryMode);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CellId)
            .SetPresent(tablet->GetCell()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ActionId)
            .SetPresent(tablet->GetAction()));
        descriptors->push_back(EInternedAttributeKey::RetainedTimestamp);
        descriptors->push_back(EInternedAttributeKey::UnflushedTimestamp);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Errors)
            .SetOpaque(true));
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* tablet = GetThisImpl();
        const auto* table = tablet->GetTable();
        const auto* chunkList = tablet->GetChunkList();

        const auto& tabletManager = Bootstrap_->GetTabletManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& cypressManager = Bootstrap_->GetCypressManager();

        switch (key) {
            case EInternedAttributeKey::State:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetState());
                return true;

            case EInternedAttributeKey::Statistics:
                BuildYsonFluently(consumer)
                    .Value(New<TSerializableTabletStatistics>(
                        tabletManager->GetTabletStatistics(tablet),
                        chunkManager));
                return true;

            case EInternedAttributeKey::TablePath:
                if (!IsObjectAlive(tablet->GetTable())) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(cypressManager->GetNodePath(
                        tablet->GetTable()->GetTrunkNode(),
                        nullptr));
                return true;

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

            case EInternedAttributeKey::MountRevision:
                if (!tablet->GetCell()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(tablet->GetMountRevision());
                return true;

            case EInternedAttributeKey::StoresUpdatePreparedTransactionId:
                if (!tablet->GetStoresUpdatePreparedTransaction()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(tablet->GetStoresUpdatePreparedTransaction()->GetId());
                return true;

            case EInternedAttributeKey::Index:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetIndex());
                return true;

            case EInternedAttributeKey::TableId:
                BuildYsonFluently(consumer)
                    .Value(table->GetId());
                return true;

            case EInternedAttributeKey::PivotKey:
                if (!table->IsPhysicallySorted()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(tablet->GetPivotKey());
                return true;

            case EInternedAttributeKey::ChunkListId:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetChunkList()->GetId());
                return true;

            case EInternedAttributeKey::InMemoryMode:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetInMemoryMode());
                return true;

            case EInternedAttributeKey::CellId:
                if (!tablet->GetCell()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(tablet->GetCell()->GetId());
                return true;

            case EInternedAttributeKey::ActionId:
                if (!tablet->GetAction()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(tablet->GetAction()->GetId());
                return true;

            case EInternedAttributeKey::RetainedTimestamp:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetRetainedTimestamp());
                return true;

            case EInternedAttributeKey::UnflushedTimestamp:
                BuildYsonFluently(consumer)
                    .Value(static_cast<TTimestamp>(tablet->NodeStatistics().unflushed_timestamp()));
                return true;

            case EInternedAttributeKey::Errors:
                BuildYsonFluently(consumer)
                    .Value(tablet->GetErrors());
                return true;

            default:
                break;
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

