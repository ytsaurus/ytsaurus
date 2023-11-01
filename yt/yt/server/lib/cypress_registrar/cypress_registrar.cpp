#include "cypress_registrar.h"
#include "config.h"
#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TCypressRegistrar
    : public ICypressRegistrar
{
public:
    TCypressRegistrar(
        TCypressRegistrarOptions&& options,
        TCypressRegistrarConfigPtr config,
        IClientPtr client,
        IInvokerPtr invoker)
        : Options_(std::move(options))
        , Config_(std::move(config))
        , Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , Executor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TCypressRegistrar::DoUpdateNodesGuarded, MakeWeak(this)),
            Config_->UpdatePeriod))
        , RootPath_(Options_.RootPath)
        , OrchidPath_(RootPath_ + "/orchid")
        , Logger(CypressRegistrarLogger.WithTag("RootPath: %v", RootPath_))
    {
        try {
            if (Options_.RootPath.empty()) {
                THROW_ERROR_EXCEPTION("\"root_path\" cannot be empty");
            }

            if (Options_.ExpireSelf && !Config_->RootNodeTtl) {
                THROW_ERROR_EXCEPTION("\"root_node_ttl\" must be set if \"expire_self\" is set");
            }

            if (Options_.CreateAliveChild && !Config_->AliveChildTtl) {
                THROW_ERROR_EXCEPTION("\"alive_child_ttl\" must be set if \"create_alive_child\" is set");
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to create Cypress registrar")
                << ex;
        }
    }

    void Start(TCallback<IAttributeDictionaryPtr()> attributeFactory) override
    {
        YT_VERIFY(!Started_);
        Started_ = true;
        AttributeFactory_ = std::move(attributeFactory);
        Executor_->Start();
    }

    TFuture<void> CreateNodes() override
    {
        YT_VERIFY(!Started_);

        return BIND(&TCypressRegistrar::DoCreateNodes, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run()
            .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    YT_LOG_ERROR(error, "Failed to create nodes");
                    THROW_ERROR_EXCEPTION("Cypress registrar failed to create nodes")
                        << error;
                }
            }));
    }

    TFuture<void> UpdateNodes(NYTree::IAttributeDictionaryPtr attributes) override
    {
        YT_VERIFY(!Started_);

        return BIND(&TCypressRegistrar::DoUpdateNodes, MakeStrong(this), std::move(attributes))
            .AsyncVia(Invoker_)
            .Run()
            .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
                if (!error.IsOK()) {
                    YT_LOG_ERROR(error, "Failed to create nodes");
                    THROW_ERROR_EXCEPTION("Cypress registrar failed to update nodes")
                        << error;
                }
            }));
    }

private:
    const TCypressRegistrarOptions Options_;
    const TCypressRegistrarConfigPtr Config_;
    TCallback<IAttributeDictionaryPtr()> AttributeFactory_;
    const IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const TPeriodicExecutorPtr Executor_;

    // Aliases for convenience.
    const TYPath RootPath_;
    const TYPath OrchidPath_;

    const NLogging::TLogger Logger;

    bool Started_ = false;
    bool Initialized_ = false;

    template <class TOptions>
    void SetRequestOptions(TOptions& options) const
    {
        options.SuppressUpstreamSync = Options_.SuppressUpstreamSync;
        options.SuppressTransactionCoordinatorSync = Options_.SuppressTransactionCoordinatorSync;
        options.Timeout = Config_->RequestTimeout;
    }

    IAttributeDictionaryPtr WithExpirationTime(IAttributeDictionaryPtr attributes) const
    {
        if (Options_.ExpireSelf) {
            YT_VERIFY(Config_->RootNodeTtl);
            attributes = attributes ? attributes->Clone() : CreateEphemeralAttributes();
            attributes->Set("expiration_time", TInstant::Now() + *Config_->RootNodeTtl);
        }

        return attributes;
    }

    void CreateAliveChild()
    {
        YT_VERIFY(Config_->AliveChildTtl);

        TCreateNodeOptions options;
        SetRequestOptions(options);
        options.Force = true;
        options.Attributes = BuildAttributeDictionaryFluently()
            .Item("expiration_time").Value(TInstant::Now() + *Config_->AliveChildTtl)
            .Finish();
        WaitFor(Client_->CreateNode(RootPath_ + "/" + AliveNodeName, EObjectType::MapNode, options))
            .ThrowOnError();

        YT_LOG_INFO("Liveness updated");
    }

    void DoCreateNodes()
    {
        YT_LOG_INFO("Started creating nodes");

        bool createdRoot = false;

        // Create root node.
        if (!WaitFor(Client_->NodeExists(RootPath_)).ValueOrThrow()) {
            YT_LOG_DEBUG("Creating new nodes");
            createdRoot = true;

            TCreateNodeOptions options;
            SetRequestOptions(options);
            options.Recursive = true;
            options.Attributes = Options_.AttributesOnCreation;
            auto errorOrId = WaitFor(Client_->CreateNode(RootPath_, Options_.NodeType, options));
            // COMPAT(kvk1920): Remove after all masters are updated to 23.2.
            if (errorOrId.GetMessage().Contains("EObjectType(1500)")) {
                // Old master does not support this type.
                YT_LOG_DEBUG(
                    errorOrId,
                    "Failed to create cluster proxy node; fall back to map node");
                errorOrId = WaitFor(Client_->CreateNode(RootPath_, EObjectType::MapNode, options));
            } else {
                YT_LOG_DEBUG("Cluster proxy node successfully created");
            }
            errorOrId.ThrowOnError();

            YT_LOG_INFO("Root node created");
        } else if (Options_.NodeType == EObjectType::ClusterProxyNode) {
            // COMPAT(kvk1920): Remove after all masters are updated to 23.2.
            TGetNodeOptions getOptions;
            getOptions.Attributes = {"type", "user_attributes"};
            auto result = ConvertToNode(WaitFor(Client_->GetNode(RootPath_ + "/@", getOptions))
                .ValueOrThrow())->AsMap();
            auto type = result->FindChildValue<TString>("type");
            YT_LOG_DEBUG("Existing entry type: %Qv",
                type);
            if (type == "map_node") {
                YT_LOG_INFO("Trying to recreate root node as %Qlv",
                    Options_.NodeType);

                TCreateNodeOptions createNodeOptions;
                SetRequestOptions(createNodeOptions);
                createNodeOptions.Recursive = true;
                createNodeOptions.Attributes = Options_.AttributesOnCreation->Clone();
                auto userAttributes = result->FindChild("user_attributes")->AsMap();
                userAttributes->RemoveChild("maintenance_requests");
                createNodeOptions.Attributes->MergeFrom(userAttributes);
                createNodeOptions.Force = true;

                auto errorOrId = WaitFor(Client_->CreateNode(RootPath_, Options_.NodeType, createNodeOptions));

                if (errorOrId.IsOK()) {
                    YT_LOG_INFO("Root node recreated");
                } else if (errorOrId.GetMessage().Contains("EObjectType(1500)")) {
                    YT_LOG_INFO("Root node is not recreated: cluster still does not support \"cluster_proxy_node\"");
                } else {
                    errorOrId.ThrowOnError();
                }
            }
        }

        // Create orchid node.
        if (Options_.OrchidRemoteAddresses &&
            (createdRoot || !WaitFor(Client_->NodeExists(OrchidPath_)).ValueOrThrow()))
        {
            TCreateNodeOptions options;
            SetRequestOptions(options);
            options.Attributes = BuildAttributeDictionaryFluently()
                .Item("remote_addresses").Value(*Options_.OrchidRemoteAddresses)
                .Finish();

            WaitFor(Client_->CreateNode(OrchidPath_, EObjectType::Orchid, options))
                .ThrowOnError();

            YT_LOG_INFO("Orchid node created");
        }

        // Set root node initial attributes.
        if (auto attributes = WithExpirationTime(Options_.AttributesOnStart)) {
            TMultisetAttributesNodeOptions options;
            SetRequestOptions(options);
            WaitFor(Client_->MultisetAttributesNode(RootPath_ + "/@", attributes->ToMap(), options))
                .ThrowOnError();
            YT_LOG_INFO("Initial attributes set");
        }

        // Create |alive| child.
        if (Options_.CreateAliveChild) {
            CreateAliveChild();
        }

        YT_LOG_INFO("Finished creating nodes");
    }

    bool IsMigrationNeeded()
    {
        if (Options_.NodeType != EObjectType::ClusterProxyNode) {
            return false;
        }

        auto rsp = WaitFor(Client_->GetNode(RootPath_ + "/@type", {}));
        if (!rsp.IsOK()) {
            return false;
        }

        return ConvertTo<TString>(rsp.Value()) == "map_node";
    }

    void DoUpdateNodes(IAttributeDictionaryPtr attributes)
    {
        YT_LOG_INFO("Started updating nodes");

        // Create nodes if needed.
        if (Options_.EnableImplicitInitialization && (!Initialized_ || IsMigrationNeeded())) {
            // NB: may throw.
            DoCreateNodes();
            Initialized_ = true;
        }

        // Update root node expiration time and attributes..
        if (auto patchedAttributes = WithExpirationTime(attributes)) {
            TMultisetAttributesNodeOptions options;
            SetRequestOptions(options);
            auto rsp = WaitFor(Client_->MultisetAttributesNode(RootPath_ + "/@", patchedAttributes->ToMap(), options));
            if (!rsp.IsOK()) {
                if (Options_.ExpireSelf &&
                    Options_.EnableImplicitInitialization &&
                    rsp.FindMatching(NYTree::EErrorCode::ResolveError))
                {
                    YT_LOG_WARNING("Root node has expired, will reinitialize");
                    Initialized_ = false;
                }
                rsp.ThrowOnError();
            }
        }

        // Update |alive| child.
        if (Options_.CreateAliveChild) {
            CreateAliveChild();
        }

        YT_LOG_INFO("Finished updating nodes");
    }

    void DoUpdateNodesGuarded()
    {
        try {
            auto attributes = AttributeFactory_
                ? AttributeFactory_()
                : nullptr;
            DoUpdateNodes(attributes);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to update nodes");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressRegistrarPtr CreateCypressRegistrar(
    TCypressRegistrarOptions&& options,
    TCypressRegistrarConfigPtr config,
    IClientPtr client,
    IInvokerPtr invoker)
{
    return New<TCypressRegistrar>(
        std::move(options),
        std::move(config),
        std::move(client),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
