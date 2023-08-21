#include "hunk_storage_node_type_handler.h"

#include "hunk_storage_node.h"
#include "hunk_storage_node_proxy.h"
#include "tablet_manager.h"
#include "tablet_owner_type_handler_base.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/cypress_server/config.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

namespace NYT::NTabletServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NCypressServer;
using namespace NJournalClient;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class THunkStorageNodeTypeHandler
    : public TTabletOwnerTypeHandlerBase<THunkStorageNode>
{
public:
    explicit THunkStorageNodeTypeHandler(TBootstrap* bootstrap)
        : TBase(bootstrap)
        , Bootstrap_(bootstrap)
    { }

protected:
    using TBase = TTabletOwnerTypeHandlerBase<THunkStorageNode>;

    TBootstrap* const Bootstrap_;

    EObjectType GetObjectType() const override
    {
        return EObjectType::HunkStorage;
    }

    ICypressNodeProxyPtr DoGetProxy(THunkStorageNode* hunkStorage, TTransaction* transaction) override
    {
        return CreateHunkStorageNodeProxy(Bootstrap_, &Metadata_, transaction, hunkStorage);
    }

    std::unique_ptr<THunkStorageNode> DoCreate(
        NCypressServer::TVersionedNodeId id,
        const NCypressServer::TCreateNodeContext& context) override
    {
        auto combinedAttributes = OverlayAttributeDictionaries(context.ExplicitAttributes, context.InheritedAttributes);

        const auto& config = Bootstrap_->GetConfig()->CypressManager;
        auto erasureCodec = combinedAttributes->GetAndRemove<NErasure::ECodec>("erasure_codec", config->DefaultJournalErasureCodec);
        auto replicationFactor = combinedAttributes->GetAndRemove<int>("replication_factor", config->DefaultJournalReplicationFactor);
        auto readQuorum = combinedAttributes->GetAndRemove<int>("read_quorum", config->DefaultJournalReadQuorum);
        auto writeQuorum = combinedAttributes->GetAndRemove<int>("write_quorum", config->DefaultJournalWriteQuorum);

        auto optionalTabletCellBundleName = combinedAttributes->FindAndRemove<TString>("tablet_cell_bundle");
        auto tabletCount = combinedAttributes->GetAndRemove<int>("tablet_count", 1);

        const auto& tabletManager = this->Bootstrap_->GetTabletManager();
        auto* tabletCellBundle = optionalTabletCellBundleName
            ? tabletManager->GetTabletCellBundleByNameOrThrow(*optionalTabletCellBundleName, true /*activeLifeStageOnly*/)
            : tabletManager->GetDefaultTabletCellBundle();

        auto nodeHolder = this->DoCreateImpl(
            id,
            context,
            replicationFactor,
            /*compressionCodec*/ NCompression::ECodec::None,
            erasureCodec,
            /*enableStripedErasure*/ false);
        auto* node = nodeHolder.get();

        try {
            node->SetReadQuorum(readQuorum);
            node->SetWriteQuorum(writeQuorum);

            tabletManager->SetTabletCellBundle(node, tabletCellBundle);

            tabletManager->PrepareReshard(
                node,
                /*firstTabletIndex*/ -1,
                /*lastTabletIndex*/ -1,
                /*newTabletCount*/ tabletCount,
                /*pivotKeys*/ {},
                /*create*/ true);
            tabletManager->Reshard(
                node,
                /*firstTabletIndex*/ -1,
                /*lastTabletIndex*/ -1,
                /*newTabletCount*/ tabletCount,
                /*pivotKeys*/ {});
        } catch (const std::exception&) {
            this->Destroy(node);
            throw;
        }

        return nodeHolder;
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateHunkStorageTypeHandler(TBootstrap* bootstrap)
{
    return New<THunkStorageNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
