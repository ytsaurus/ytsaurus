#include "cross_cluster_replicated_value.h"

#include "cross_cluster_client.h"
#include "cross_cluster_replica_lock_waiter.h"
#include "config.h"

#include <yt/yt/client/object_client/helpers.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NCrossClusterReplicatedState {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NLogging;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReplicaUpdater
    : public TRefCounted
{
public:
    TReplicaUpdater(
        ICrossClusterReplicaLockWaiterPtr lockWaiter,
        ISingleClusterClientPtr clientWrapper,
        TCrossClusterReplicatedStateConfigPtr config,
        TYPath nodePath,
        INodePtr node,
        std::string tag,
        TLogger logger)
        : LockWaiter_(std::move(lockWaiter))
        , Client_(std::move(clientWrapper))
        , Config_(std::move(config))
        , NodePath_(std::move(nodePath))
        , Node_(std::move(node))
        , Tag_(std::move(tag))
        , Logger(std::move(logger))
    { }

    TFuture<void> Update()
    {
        return CreateNode()
            .Apply(BIND(&TReplicaUpdater::StartTransaction, MakeStrong(this)))
            .Apply(BIND(&TReplicaUpdater::LockNode, MakeStrong(this)))
            .Apply(BIND(&TReplicaUpdater::WaitForLock, MakeStrong(this)))
            .Apply(BIND(&TReplicaUpdater::DoUpdate, MakeStrong(this)));
    }

private:
    ICrossClusterReplicaLockWaiterPtr LockWaiter_;
    ISingleClusterClientPtr Client_;
    TCrossClusterReplicatedStateConfigPtr Config_;
    TYPath NodePath_;
    INodePtr Node_;

    ITransactionPtr Transaction_;
    TLockId LockId_;
    bool AcquiredLock_{false};
    TReplicaVersion CurrentVersion_;

    std::string Tag_;
    TLogger Logger;

    TFuture<void> CreateNode()
    {
        TCreateNodeOptions createOptions;
        createOptions.Timeout = Config_->RequestTimeout;
        createOptions.IgnoreExisting = true;
        return Client_->GetClient()->CreateNode(NodePath_, NObjectClient::EObjectType::Document, createOptions)
            .AsVoid();
    }

    TFuture<void> StartTransaction()
    {
        auto startAttributes = CreateEphemeralAttributes();
        startAttributes->Set("timeout", Config_->UpdateTransactionTimeout);
        auto txStartOptions = TTransactionStartOptions{
            .Ping = false,
            .Attributes = std::move(startAttributes),
        };
        return Client_->GetClient()->StartTransaction(NTransactionClient::ETransactionType::Master, txStartOptions)
            .AsUnique()
            .Apply(BIND([this, this_ = MakeStrong(this)] (ITransactionPtr&& tx) {
                Transaction_ = std::move(tx);
                YT_LOG_DEBUG(
                    "Started update transaction (ReplicaIndex: %v, ReplicaVersionTag: %v)",
                    Client_->GetIndex(),
                    Tag_);
            }));
    }

    TFuture<void> LockNode()
    {
        TLockNodeOptions lockOptions;
        lockOptions.Timeout = Config_->RequestTimeout;
        lockOptions.Waitable = true;
        return Transaction_->LockNode(NodePath_, NCypressClient::ELockMode::Exclusive, lockOptions)
            .AsUnique()
            .Apply(BIND([this_ = MakeStrong(this)] (TLockNodeResult&& lock) {
                this_->LockId_ = lock.LockId;
            }));
    }

    TFuture<void> WaitForLock()
    {
        auto waitCallback = [this, this_ = MakeStrong(this)] (const ISingleClusterClientPtr&) {
            return LockWaiter_->WaitForLockAcquision(
                LockId_, Client_, Transaction_, NodePath_, ExtractVersion(Node_), Config_->LockWaitingTimeout);
        };
        return Client_->ExecuteCallback(Tag_, BIND(std::move(waitCallback)))
            .Apply(BIND([this_ = MakeStrong(this)] (ELockAcquisionState acquired) {
                this_->AcquiredLock_ = acquired == ELockAcquisionState::Acquired;
            }));
    }

    TFuture<void> DoUpdate()
    {
        if (!AcquiredLock_) {
            YT_LOG_DEBUG("Outdated version, no reason to wait for lock (ReplicaIndex: %v, ReplicaVersionTag: %v)",
                Client_->GetIndex(),
                Tag_);
            return Transaction_->Abort();
        }
        YT_LOG_DEBUG(
            "Lock acquired (ReplicaIndex: %v, ReplicaVersionTag: %v)",
            Client_->GetIndex(),
            Tag_);
        return GetCurrentVersion()
            .Apply(BIND(&TReplicaUpdater::SetValueOrAbort, MakeStrong(this)));
    }

    TFuture<void> GetCurrentVersion()
    {
        TGetNodeOptions getOptions;
        getOptions.Timeout = Config_->RequestTimeout;
        return Transaction_->GetNode(NodePath_, getOptions)
            .AsUnique()
            .Apply(BIND([this_ = MakeStrong(this)] (NYson::TYsonString&& currentValue) {
                auto currentNode = NYTree::ConvertToNode(currentValue);
                this_->CurrentVersion_ = ExtractVersion(currentNode);
            }));
    }

    TFuture<void> SetValueOrAbort()
    {
        auto newVersion = ExtractVersion(Node_);
        if (CurrentVersion_ >= newVersion) {
            YT_LOG_DEBUG(
                "Outdated version, aborting transaction (ReplicaIndex: %v, CurrentVersion: %v, NewVersion: %v)",
                Client_->GetIndex(),
                CurrentVersion_,
                newVersion);
            return Transaction_->Abort();
        }
        YT_LOG_DEBUG(
            "Setting new value (ReplicaIndex: %v, CurrentVersion: %v, NewVersion: %v)",
            Client_->GetIndex(),
            CurrentVersion_,
            newVersion);

        auto setCallback = [this, this_ = MakeStrong(this)] (const ISingleClusterClientPtr&) {
            return SetValue()
                .Apply(BIND(&TReplicaUpdater::SetVersionAttribute, MakeStrong(this)))
                .Apply(BIND(&TReplicaUpdater::Commit, MakeStrong(this)))
                .Apply(BIND([this, this_ = MakeStrong(this)] {
                    YT_LOG_DEBUG(
                        "Committed update (ReplicaIndex: %v, ReplicaVersionTag: %v)",
                        Client_->GetIndex(),
                        Tag_);
                    return std::monostate();
                }));
        };
        return Client_->ExecuteCallback(Tag_, BIND(std::move(setCallback)))
            .AsVoid();
    }

    TFuture<void> SetValue()
    {
        TSetNodeOptions setOptions;
        setOptions.Timeout = Config_->RequestTimeout;
        return Transaction_->SetNode(
            NodePath_, NYson::ConvertToYsonString(Node_), setOptions);
    }

    TFuture<void> SetVersionAttribute()
    {
        TSetNodeOptions setOptions;
        setOptions.Timeout = Config_->RequestTimeout;

        auto attributePath = NodePath_ + "/@" + ICrossClusterReplicatedValue::VersionAttributeKey;
        auto attributeValue = MakeVersionAttributeValue(ExtractVersion(Node_));
        return Transaction_->SetNode(
            attributePath, NYson::ConvertToYsonString(attributeValue), setOptions);
    }

    TFuture<void> Commit()
    {
        return Transaction_->Commit()
            .AsVoid();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TCrossClusterReplicatedValue
    : public ICrossClusterReplicatedValue
{
public:
    explicit TCrossClusterReplicatedValue(
        IMultiClusterClientPtr client,
        ICrossClusterReplicaLockWaiterPtr lockWaiter,
        TCrossClusterReplicatedStateConfigPtr config,
        std::string tag,
        std::vector<TYPath> paths,
        TLogger logger)
        : Client_(std::move(client))
        , LockWaiter_(std::move(lockWaiter))
        , Config_(std::move(config))
        , Tag_(std::move(tag))
        , Paths_(std::move(paths))
        , Logger(std::move(logger))
    { }

    TFuture<INodePtr> Load() override
    {
        auto processFetched = [this, this_ = MakeStrong(this)] (
            std::vector<TErrorOr<std::pair<TReplicaVersion, INodePtr>>>&& nodes)
        {
            std::pair<TReplicaVersion, INodePtr> latestNode;
            for (const auto& node : nodes) {
                if (node.IsOK() && node.Value().first > latestNode.first) {
                    latestNode = node.Value();
                }
            }

            if (latestNode.first == NullReplicaVersion) {
                return MakeFuture(INodePtr());
            }

            std::vector<TErrorOr<bool>> shouldIngoreReplica;
            auto requiresUpdate = false;
            for (auto& node : nodes) {
                if (node.IsOK()) {
                    if (node.Value().first == latestNode.first) {
                        shouldIngoreReplica.emplace_back(true);
                    } else {
                        requiresUpdate = true;
                        shouldIngoreReplica.emplace_back(false);
                    }
                } else {
                    shouldIngoreReplica.emplace_back(static_cast<const TError&>(node));
                }
            }

            if (!requiresUpdate) {
                return MakeFuture(std::move(latestNode.second));
            }
            YT_LOG_DEBUG("Found inconsistency, updating (ReplicaVersionTag: %v)", Tag_);
            return this_->UpdateNodes(this_->Client_, latestNode.second, std::move(shouldIngoreReplica))
                .Apply(BIND([latestNode = latestNode.second] {
                    return latestNode;
                }));
        };

        return FetchNodes(Client_)
            .AsUnique()
            .Apply(BIND(std::move(processFetched)));
    }

    TFuture<void> Store(const NYTree::IMapNodePtr& value) override
    {
        auto processFetched = [this, this_ = MakeStrong(this), value] (
            std::vector<TErrorOr<std::pair<TReplicaVersion, INodePtr>>>&& nodes)
        {
            auto latestVersion = NullReplicaVersion;
            for (const auto& node : nodes) {
                if (node.IsOK()) {
                    latestVersion = std::max(latestVersion, node.Value().first);
                }
            }

            PutVersion(value, {latestVersion.first + 1, Tag_});

            std::vector<TErrorOr<bool>> replicasToIgnore;
            replicasToIgnore.reserve(nodes.size());
            for (auto& node : nodes) {
                if (node.IsOK()) {
                    replicasToIgnore.emplace_back(false);
                } else {
                    replicasToIgnore.emplace_back(static_cast<const TError&>(node));
                }
            }

            return UpdateNodes(Client_, value, std::move(replicasToIgnore));
        };

        return FetchNodes(Client_)
            .AsUnique()
            .Apply(BIND(std::move(processFetched)));
    }

private:
    IMultiClusterClientPtr Client_;
    ICrossClusterReplicaLockWaiterPtr LockWaiter_;
    TCrossClusterReplicatedStateConfigPtr Config_;
    std::string Tag_;
    std::vector<TYPath> Paths_;
    TLogger Logger;

    TFuture<std::vector<TErrorOr<std::pair<TReplicaVersion, INodePtr>>>> FetchNodes(
        const IMultiClusterClientPtr& client)
    {
        auto getNode = [this, this_ = MakeStrong(this)] (const ISingleClusterClientPtr& client) {
            TGetNodeOptions getOptions;
            getOptions.Timeout = Config_->RequestTimeout;
            return client->GetClient()->GetNode(Paths_[client->GetIndex()], getOptions)
                .AsUnique()
                .Apply(BIND([] (TYsonString&& getResult) {
                    try {
                        auto node = ConvertToNode(getResult);
                        auto version = ExtractVersion(node);
                        return std::pair(version, node);
                    } catch (const TErrorException& e) {
                        THROW_ERROR_EXCEPTION(
                            EErrorCode::InvalidNodeFormat,
                            "Invalid node format");
                    }
                }));
        };

        return client->ExecuteCallback(Tag_, BIND(std::move(getNode)))
            .AsUnique()
            .Apply(BIND(&TCrossClusterReplicatedValue::ProcessGets, MakeStrong(this)));
    }

    TFuture<void> UpdateNodes(
        const IMultiClusterClientPtr& client,
        const INodePtr& node,
        std::vector<TErrorOr<bool>> shouldIngoreReplica)
    {
        auto updateNode = [
            this,
            this_ = MakeStrong(this),
            node,
            shouldIngoreReplica = std::move(shouldIngoreReplica)
        ] (const ISingleClusterClientPtr& client) {
            auto index = client->GetIndex();
            if (!shouldIngoreReplica[index].IsOK()) {
                return MakeFuture<std::monostate>(static_cast<const TError&>(shouldIngoreReplica[index]));
            }
            if (shouldIngoreReplica[index].Value()) {
                return MakeFuture(std::monostate());
            }

            auto path = Paths_[index];
            auto updater = New<TReplicaUpdater>(LockWaiter_, client, Config_, path, node, Tag_, Logger);
            return updater->Update()
                .Apply(BIND([this_] { return std::monostate(); }));
        };

        return client->ExecuteCallback(Tag_, BIND(std::move(updateNode)))
            .AsUnique()
            .Apply(BIND(&TCrossClusterReplicatedValue::ProcessUpdates, MakeStrong(this)));
    }

    std::vector<TErrorOr<std::pair<TReplicaVersion, INodePtr>>> ProcessGets(
        std::vector<TErrorOr<std::pair<TReplicaVersion, INodePtr>>>&& getResults)
    {
        YT_LOG_DEBUG("Processing gets (ReplicaVersionTag: %v)", Tag_);
        std::vector<int> failedClusters;
        std::vector<TError> innerErrors;
        for (auto&& [index, result] : SEnumerate(getResults)) {
            if (!result.IsOK()) {
                if (result.GetCode() == NYTree::EErrorCode::ResolveError) {
                    getResults[index] = std::pair(NullReplicaVersion, INodePtr());
                } else {
                    YT_LOG_DEBUG(
                        result,
                        "Fetching error (ReplicaIndex: %v, ReplicaVersionTag: %v)", index, Tag_);
                    failedClusters.emplace_back(index);
                    innerErrors.push_back(static_cast<const TError&>(result));
                }
            }
        }

        if (failedClusters.size() * 2 >= getResults.size()) {
            auto error = TError("Can't get values from cluster quorum")
                << TErrorAttribute("failed_cluster_indices", failedClusters);
            *error.MutableInnerErrors() = std::move(innerErrors);
            THROW_ERROR std::move(error);
        }

        return std::move(getResults);
    }

    void ProcessUpdates(std::vector<TErrorOr<std::monostate>>&& updateResults)
    {
        std::vector<int> failedClusters;
        std::vector<TError> innerErrors;
        for (auto&& [index, result] : SEnumerate(updateResults)) {
            if (!result.IsOK()) {
                YT_LOG_DEBUG(
                    result,
                    "Updating error (ReplicaIndex: %v, ReplicaVersionTag: %v)", index, Tag_);
                failedClusters.emplace_back(index);
                innerErrors.push_back(static_cast<const TError&>(result));
            }
        }

        if (failedClusters.size() * 2 >= updateResults.size()) {
            auto error = TError("Can't update values on cluster quorum")
                << TErrorAttribute("failed_cluster_indices", failedClusters);
            *error.MutableInnerErrors() = std::move(innerErrors);
            THROW_ERROR std::move(error);
        }
    }
};

ICrossClusterReplicatedValuePtr CreateCrossClusterReplicatedValue(
    IMultiClusterClientPtr client,
    ICrossClusterReplicaLockWaiterPtr lockWaiter,
    TCrossClusterReplicatedStateConfigPtr config,
    std::string tag,
    std::vector<TYPath> paths,
    TLogger logger)
{
    return New<TCrossClusterReplicatedValue>(
        std::move(client),
        std::move(lockWaiter),
        std::move(config),
        std::move(tag),
        std::move(paths),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
