#include "cypress_proxy_tracker.h"

#include "private.h"

#include "cypress_integration.h"
#include "cypress_proxy_object.h"
#include "cypress_proxy_type_handler.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/lib/sequoia/proto/cypress_proxy_tracker.pb.h>

#include <yt/yt/core/misc/id_generator.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectServer;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NThreading;

using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

constexpr static auto& Logger = SequoiaServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyTracker
    : public ICypressProxyTracker
    , public TMasterAutomatonPart
{
public:
    TCypressProxyTracker(TBootstrap* bootstrap, IChannelFactoryPtr channelFactory)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::CypressProxyTracker)
        , ChannelFactory_(std::move(channelFactory))
    {
        RegisterLoader(
            "CypressProxyTracker.Keys",
            BIND_NO_PROPAGATE(&TCypressProxyTracker::LoadKeys, Unretained(this)));
        RegisterLoader(
            "CypressProxyTracker.Values",
            BIND_NO_PROPAGATE(&TCypressProxyTracker::LoadValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TCypressProxyTracker::HydraCypressProxyHeartbeat, Unretained(this)));
    }

    void ProcessCypressProxyHeartbeat(const TCtxHeartbeatPtr& context) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto mutation = CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            context,
            &TCypressProxyTracker::HydraCypressProxyHeartbeat,
            this);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndReply(context));
    }

    void Initialize() override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateCypressProxyTypeHandler(Bootstrap_, &CypressProxyMap_));

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        cypressManager->RegisterHandler(CreateCypressProxyMapTypeHandler(Bootstrap_));
    }

    TCypressProxyObject* FindCypressProxyByAddress(const std::string& address) override
    {
        return GetOrDefault(CypressProxyByAddress_, address, nullptr);
    }

    IChannelPtr GetCypressProxyChannelOrThrow(const std::string& address) override
    {
        VerifyPersistentStateRead();

        if (!FindCypressProxyByAddress(address)) {
            THROW_ERROR_EXCEPTION("No such Cypress proxy %Qv", address);
        }

        return ChannelFactory_->CreateChannel(address);
    }

    DECLARE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS_OVERRIDE(CypressProxy, CypressProxies, TCypressProxyObject);

private:
    const IChannelFactoryPtr ChannelFactory_;

    // Persistent.
    THashMap<std::string, TCypressProxyObject*> CypressProxyByAddress_;
    TEntityMap<TCypressProxyObject> CypressProxyMap_;

    void Clear() override
    {
        CypressProxyByAddress_ = {};
        CypressProxyMap_.Clear();
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        CypressProxyMap_.SaveKeys(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        CypressProxyMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        CypressProxyMap_.LoadValues(context);
    }

    void OnAfterSnapshotLoaded() override
    {
        CypressProxyByAddress_.reserve(CypressProxyMap_.size());

        for (const auto& [objectId, cypressProxy] : CypressProxyMap_) {
            RegisterCypressProxy(cypressProxy);
        }
    }

    void RegisterCypressProxy(TCypressProxyObject* proxyObject)
    {
        EmplaceOrCrash(CypressProxyByAddress_, proxyObject->GetAddress(), proxyObject);
    }

    void UnregisterCypressProxy(TCypressProxyObject* proxyObject)
    {
        EraseOrCrash(CypressProxyByAddress_, proxyObject->GetAddress());
    }

    TCypressProxyObject* CreateCypressProxy(const std::string& address)
    {
        YT_VERIFY(HasMutationContext());
        YT_VERIFY(Bootstrap_->IsPrimaryMaster());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::CypressProxyObject);

        auto holder = TPoolAllocator::New<TCypressProxyObject>(id);
        holder->SetAddress(address);

        auto* proxyObject = CypressProxyMap_.Insert(id, std::move(holder));
        RegisterCypressProxy(proxyObject);

        proxyObject->RefObject();

        return proxyObject;
    }

    void HydraCypressProxyHeartbeat(
        const TCtxHeartbeatPtr& context,
        TReqHeartbeat* request,
        TRspHeartbeat* /*response*/)
    {
        YT_VERIFY(Bootstrap_->IsPrimaryMaster());

        // NB: static_cast is intended. We do nothing with this value but
        // comparing with current reign.
        auto sequoiaReign = static_cast<ESequoiaReign>(request->sequoia_reign());
        const auto& address = request->address();

        auto* proxy = FindCypressProxyByAddress(address);
        if (!proxy) {
            proxy = CreateCypressProxy(address);
        }
        YT_VERIFY(proxy->GetAddress() == address);

        proxy->SetLastSeenTime(GetCurrentMutationContext()->GetTimestamp());
        proxy->SetSequoiaReign(sequoiaReign);

        if (context) {
            auto error = CheckSequoiaReign(sequoiaReign);
            if (!error.IsOK()) {
                YT_LOG_ALERT(error, "Attempt to register Cypress proxy with invalid Reign");
                context->Reply(std::move(error));
            }
        }
    }

    void ZombifyCypressProxy(TCypressProxyObject* proxyObject) noexcept override
    {
        UnregisterCypressProxy(proxyObject);
    }
};

DEFINE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(
    TCypressProxyTracker,
    CypressProxy,
    CypressProxies,
    TCypressProxyObject,
    CypressProxyMap_);

////////////////////////////////////////////////////////////////////////////////

ICypressProxyTrackerPtr CreateCypressProxyTracker(
    TBootstrap* bootstrap,
    IChannelFactoryPtr channelFactory)
{
    return New<TCypressProxyTracker>(bootstrap, std::move(channelFactory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
