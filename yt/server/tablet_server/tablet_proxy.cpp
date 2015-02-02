#include "stdafx.h"
#include "tablet_proxy.h"
#include "tablet.h"
#include "tablet_cell.h"
#include "tablet_manager.h"
#include "private.h"

#include <core/yson/consumer.h>

#include <core/ytree/fluent.h>

#include <server/object_server/object_detail.h>

#include <server/table_server/table_node.h>

#include <server/chunk_server/chunk_list.h>

#include <server/cell_master/bootstrap.h>

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

    virtual NLog::TLogger CreateLogger() const override
    {
        return TabletServerLogger;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        const auto* tablet = GetThisTypedImpl();
        attributes->push_back("state");
        attributes->push_back("statistics");
        attributes->push_back("index");
        attributes->push_back("table_id");
        attributes->push_back("pivot_key");
        attributes->push_back("chunk_list_id");
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

