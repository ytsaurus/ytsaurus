#include "chaos_cell_bundle.h"
#include "chaos_cell_bundle_proxy.h"
#include "chaos_manager.h"
#include "config.h"

#include <yt/yt/server/master/cell_server/cell_bundle_type_handler.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NChaosServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NTransactionServer;
using namespace NSecurityServer;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCellServer;

////////////////////////////////////////////////////////////////////////////////

class TChaosCellBundleTypeHandler
    : public TCellBundleTypeHandlerBase<TChaosCellBundle>
{
public:
    using TCellBundleTypeHandlerBase::TCellBundleTypeHandlerBase;

    virtual EObjectType GetType() const override
    {
        return EObjectType::ChaosCellBundle;
    }

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::ChaosCellBundle, hintId);
        auto holder = TPoolAllocator::New<TChaosCellBundle>(id);
        auto chaosOptions = attributes->GetAndRemove<TChaosHydraConfigPtr>("chaos_options");
        auto options = attributes->GetAndRemove<TTabletCellOptionsPtr>("options");
        if (options->PeerCount != std::ssize(chaosOptions->Peers)) {
            THROW_ERROR_EXCEPTION("Peer descriptors size does not match peer count");
        }
        options->IndependentPeers = true;
        holder->SetChaosOptions(std::move(chaosOptions));
        return DoCreateObject(std::move(holder), attributes, std::move(options));
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        TObjectId hintId) override
    {
        return TPoolAllocator::New<TChaosCellBundle>(hintId);
    }

private:
    using TBase = TCellBundleTypeHandlerBase<TChaosCellBundle>;

    virtual TString DoGetName(const TChaosCellBundle* cellBundle) override
    {
        return Format("chaos cell bundle %Qv", cellBundle->GetName());
    }

    virtual IObjectProxyPtr DoGetProxy(TChaosCellBundle* cellBundle, TTransaction* /*transaction*/) override
    {
        return CreateChaosCellBundleProxy(Bootstrap_, &Metadata_, cellBundle);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateChaosCellBundleTypeHandler(
    TBootstrap* bootstrap)
{
    return New<TChaosCellBundleTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
