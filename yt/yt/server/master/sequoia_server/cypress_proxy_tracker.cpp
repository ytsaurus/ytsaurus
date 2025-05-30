#include "cypress_proxy_tracker.h"

#include "private.h"

#include "config.h"

#include "cypress_integration.h"
#include "cypress_proxy_object.h"
#include "cypress_proxy_type_handler.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/cypress_server/config.h>
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

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "CypressProxyTracker.Keys",
            BIND_NO_PROPAGATE(&TCypressProxyTracker::SaveKeys, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "CypressProxyTracker.Values",
            BIND_NO_PROPAGATE(&TCypressProxyTracker::SaveValues, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TCypressProxyTracker::HydraCypressProxyHeartbeat, Unretained(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TCypressProxyTracker::OnDynamicConfigChanged, Unretained(this)));
    }

    void MaybePersistCypressProxyRegistration(const NProto::TReqHeartbeat& request)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        if (hydraManager->GetReadOnly()) {
            return;
        }

        auto mutationRequired = RegistrationCache_.Read([&] (const TRegistrationCache& cache) {
            auto it = cache.find(request.address());
            return
                it == cache.end() ||
                it->second.Reign != static_cast<ESequoiaReign>(request.sequoia_reign()) ||
                it->second.Version != request.version() ||
                NProfiling::GetInstant() > it->second.LastPersistentHeartbeatTime + PersistentHeartbeatPeriod_.load();
        });

        if (!mutationRequired) {
            return;
        }

        auto mutation = CreateMutation(hydraManager, request);
        mutation->SetCurrentTraceContext();
        mutation->SetAllowLeaderForwarding(true);
        YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger()));
    }

    void ProcessCypressProxyHeartbeat(const TCtxHeartbeatPtr& context) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        const auto& request = context->Request();
        auto& response = context->Response();

        auto reign = static_cast<ESequoiaReign>(request.sequoia_reign());
        auto error = NSequoiaServer::CheckSequoiaReign(reign);

        if (error.IsOK()) {
            response.set_master_reign(ToProto(GetCurrentReign()));
            response.mutable_limits()->set_max_copiable_subtree_size(MaxCopiableSubtreeSize_.load());
        } else {
            YT_LOG_ALERT(error, "Failed to register Cypress proxy");
        }
        context->Reply(error);

        MaybePersistCypressProxyRegistration(request);
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
        VerifyPersistentStateRead();

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

    struct TCypressProxyRegistrationInfo
    {
        ESequoiaReign Reign;
        std::string Version;
        TInstant LastPersistentHeartbeatTime;
    };
    using TRegistrationCache = THashMap<std::string, TCypressProxyRegistrationInfo>;
    TAtomicObject<TRegistrationCache> RegistrationCache_;

    // Part of dynamic config to read it from non-automaton thread.
    std::atomic<TDuration> PersistentHeartbeatPeriod_;
    std::atomic<int> MaxCopiableSubtreeSize_;

    // Persistent.
    THashMap<std::string, TCypressProxyObject*> CypressProxyByAddress_;
    TEntityMap<TCypressProxyObject> CypressProxyMap_;

    void Clear() override
    {
        CypressProxyByAddress_ = {};
        CypressProxyMap_.Clear();
        RegistrationCache_.Store(TRegistrationCache{});
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        CypressProxyMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        CypressProxyMap_.SaveValues(context);
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

        for (auto [objectId, cypressProxy] : CypressProxyMap_) {
            RegisterCypressProxy(cypressProxy);
        }
    }

    void RegisterCypressProxy(TCypressProxyObject* proxyObject)
    {
        EmplaceOrCrash(CypressProxyByAddress_, proxyObject->GetAddress(), proxyObject);
        RegistrationCache_.Transform([&] (TRegistrationCache& cache) {
            cache[proxyObject->GetAddress()] = {
                .Reign = proxyObject->GetSequoiaReign(),
                .Version = proxyObject->GetVersion(),
            };
        });
        YT_LOG_DEBUG("Cypress proxy registered (Address: %v, SequoiaReign: %v, Version: %v)",
            proxyObject->GetAddress(),
            proxyObject->GetSequoiaReign(),
            proxyObject->GetVersion());
    }

    void UnregisterCypressProxy(TCypressProxyObject* proxyObject)
    {
        EraseOrCrash(CypressProxyByAddress_, proxyObject->GetAddress());
        RegistrationCache_.Transform([&] (TRegistrationCache& cache) {
            cache.erase(proxyObject->GetAddress());
        });
        YT_LOG_DEBUG("Cypress proxy unregistered (Address: %v)", proxyObject->GetAddress());
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

    void HydraCypressProxyHeartbeat(TReqHeartbeat* request)
    {
        YT_VERIFY(HasMutationContext());
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

        proxy->SetLastPersistentHeartbeatTime(GetCurrentMutationContext()->GetTimestamp());
        proxy->SetSequoiaReign(sequoiaReign);
        proxy->SetVersion(request->version());
    }

    void ZombifyCypressProxy(TCypressProxyObject* proxyObject) noexcept override
    {
        UnregisterCypressProxy(proxyObject);
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        const auto& config = Bootstrap_->GetDynamicConfig();
        PersistentHeartbeatPeriod_.store(config->CypressProxyTracker->PersistentHeartbeatPeriod);
        MaxCopiableSubtreeSize_.store(config->CypressManager->CrossCellCopyMaxSubtreeSize);
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
