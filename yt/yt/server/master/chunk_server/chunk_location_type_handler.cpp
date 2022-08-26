#include "chunk_location_type_handler.h"
#include "chunk_location.h"
#include "chunk_location_proxy.h"
#include "data_node_tracker_internal.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

namespace NYT::NChunkServer {

using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TChunkLocationTypeHandler
    : public TObjectTypeHandlerWithMapBase<TRealChunkLocation>
{
public:
    TChunkLocationTypeHandler(
        TBootstrap* bootstrap,
        IDataNodeTrackerInternalPtr nodeTrackerInternal)
        : TObjectTypeHandlerWithMapBase(bootstrap, nodeTrackerInternal->MutableChunkLocations())
        , NodeTrackerInternal_(std::move(nodeTrackerInternal))
    { }

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::Creatable |
            ETypeFlags::Removable |
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes;
    }

    EObjectType GetType() const override
    {
        return EObjectType::ChunkLocation;
    }

    TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        auto uuid = attributes->GetAndRemove<TChunkLocationUuid>("uuid");
        return NodeTrackerInternal_->CreateChunkLocation(uuid, hintId);
    }

private:
    const IDataNodeTrackerInternalPtr NodeTrackerInternal_;

    TCellTagList DoGetReplicationCellTags(const TRealChunkLocation* /*location*/) override
    {
        return AllSecondaryCellTags();
    }

    IObjectProxyPtr DoGetProxy(TRealChunkLocation* location, TTransaction* /*transaction*/) override
    {
        return CreateChunkLocationProxy(Bootstrap_, &Metadata_, location);
    }

    void DoZombifyObject(TRealChunkLocation* location) noexcept override
    {
        NodeTrackerInternal_->DestroyChunkLocation(location);

        TObjectTypeHandlerWithMapBase::DoZombifyObject(location);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateChunkLocationTypeHandler(
    TBootstrap* bootstrap,
    IDataNodeTrackerInternalPtr nodeTrackerInternal)
{
    return New<TChunkLocationTypeHandler>(
        bootstrap,
        std::move(nodeTrackerInternal));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
