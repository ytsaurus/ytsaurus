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

#include <yt/yt/server/master/cypress_server/composite_node.h>
#include <yt/yt/server/master/cypress_server/config.h>
#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/sequoia/proto/cypress_proxy_tracker.pb.h>

#include <yt/yt/core/misc/id_generator.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectServer;
using namespace NRpc;
using namespace NSequoiaClient;
using namespace NServer;
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
            FillSupportedInheritableAttributes(&response);
        } else {
            YT_LOG_EVENT(
                Logger,
                SequoiaEnabled_.load() ? NLogging::ELogLevel::Alert : NLogging::ELogLevel::Error,
                error,
                "Failed to register Cypress proxy");
        }
        context->Reply(error);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        if (hydraManager->GetReadOnly()) {
            return;
        }

        auto mutation = CreateMutation(hydraManager, request);
        mutation->SetCurrentTraceContext();
        YT_UNUSED_FUTURE(mutation->CommitAndLog(Logger()));
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

    // Part of dynamic config to read it from non-automaton thread.
    std::atomic<int> MaxCopiableSubtreeSize_;
    std::atomic<bool> SequoiaEnabled_;

    // Persistent.
    THashMap<std::string, TCypressProxyObject*> CypressProxyByAddress_;
    TEntityMap<TCypressProxyObject> CypressProxyMap_;

    void Clear() override
    {
        CypressProxyByAddress_.clear();
        CypressProxyMap_.Clear();
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
        YT_LOG_DEBUG("Cypress proxy registered (Address: %v, SequoiaReign: %v, Version: %v)",
            proxyObject->GetAddress(),
            proxyObject->GetSequoiaReign(),
            proxyObject->GetVersion());
    }

    void UnregisterCypressProxy(TCypressProxyObject* proxyObject)
    {
        EraseOrCrash(CypressProxyByAddress_, proxyObject->GetAddress());
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
        // compare it with the current reign.
        auto sequoiaReign = static_cast<ESequoiaReign>(request->sequoia_reign());
        const auto& address = request->address();
        auto heartbeatPeriod = FromProto<TDuration>(request->heartbeat_period());

        auto* proxy = FindCypressProxyByAddress(address);
        if (!proxy) {
            proxy = CreateCypressProxy(address);
        }
        YT_VERIFY(proxy->GetAddress() == address);

        auto mutationTimestamp = GetCurrentMutationContext()->GetTimestamp();
        auto aliveUntil = mutationTimestamp + 2 * heartbeatPeriod;
        proxy->SetAliveUntil(aliveUntil);
        proxy->SetLastPersistentHeartbeatTime(mutationTimestamp);
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
        const auto& cypressManagerConfig = config->CypressManager;
        MaxCopiableSubtreeSize_.store(cypressManagerConfig->CrossCellCopyMaxSubtreeSize);
        SequoiaEnabled_.store(config->SequoiaManager->Enable);
    }

    void FillSupportedInheritableAttributes(NProto::TRspHeartbeat* response)
    {
        auto* inheritableAttributesInfo = response->mutable_supported_inheritable_attributes();

    #define XX(FieldName, AttributeKey) \
        inheritableAttributesInfo->add_inheritable_attribute_keys(EInternedAttributeKey::AttributeKey.Unintern());
        FOR_EACH_INHERITABLE_ATTRIBUTE(XX)
        XX(Account, Account)
    #undef XX

    #define XX(FieldName, AttributeKey) \
        inheritableAttributesInfo->add_inheritable_during_copy_attribute_keys(EInternedAttributeKey::AttributeKey.Unintern());
        FOR_EACH_INHERITABLE_DURING_COPY_ATTRIBUTE(XX)
        XX(Account, Account)
    #undef XX
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
