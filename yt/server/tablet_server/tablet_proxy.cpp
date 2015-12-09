#include "tablet_proxy.h"
#include "private.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_manager.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/chunk_server/chunk_list.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/server/table_server/table_node.h>

#include <yt/core/misc/common.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTabletServer {

using namespace NYson;
using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TTabletProxy
    : public TNonversionedObjectProxyBase<TTablet>
{
public:
    TTabletProxy(NCellMaster::TBootstrap* bootstrap, TTablet* tablet)
        : TBase(bootstrap, tablet)
    { }

private:
    typedef TNonversionedObjectProxyBase<TTablet> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        const auto* tablet = GetThisTypedImpl();
        attributes->push_back("state");
        attributes->push_back("statistics");
        attributes->push_back(TAttributeInfo("performance_counters", tablet->GetState() == ETabletState::Mounted));
        attributes->push_back(TAttributeInfo("mount_revision", tablet->GetState() == ETabletState::Mounted));
        attributes->push_back("index");
        attributes->push_back("table_id");
        attributes->push_back("pivot_key");
        attributes->push_back("chunk_list_id");
        attributes->push_back("in_memory_mode");
        attributes->push_back(TAttributeInfo("cell_id", tablet->GetCell()));
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* tablet = GetThisTypedImpl();
        const auto* table = tablet->GetTable();
        auto tabletManager = Bootstrap_->GetTabletManager();

        if (key == "state") {
            BuildYsonFluently(consumer)
                .Value(tablet->GetState());
            return true;
        }

        if (key == "statistics") {
            BuildYsonFluently(consumer)
                .Value(tabletManager->GetTabletStatistics(tablet));
            return true;
        }

        if (tablet->GetState() == ETabletState::Mounted) {
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

        if (key == "pivot_key") {
            BuildYsonFluently(consumer)
                .Value(tablet->GetPivotKey());
            return true;
        }

        if (key == "chunk_list_id") {
            BuildYsonFluently(consumer)
                .Value(table->GetChunkList()->Children()[tablet->GetIndex()]->GetId());
            return true;
        }

        if (key == "in_memory_mode") {
            BuildYsonFluently(consumer)
                .Value(tablet->GetInMemoryMode());
            return true;
        }

        if (tablet->GetCell()) {
            if (key == "cell_id") {
                BuildYsonFluently(consumer)
                    .Value(tablet->GetCell()->GetId());
                return true;
            }
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

};

IObjectProxyPtr CreateTabletProxy(
    NCellMaster::TBootstrap* bootstrap,
    TTablet* tablet)
{
    return New<TTabletProxy>(bootstrap, tablet);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

