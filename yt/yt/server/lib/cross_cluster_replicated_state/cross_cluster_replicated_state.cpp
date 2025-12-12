#include "cross_cluster_replicated_state.h"

#include "cross_cluster_replicated_value.h"
#include "config.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <library/cpp/iterator/enumerate.h>

namespace NYT::NCrossClusterReplicatedState {

using namespace NApi;
using namespace NLogging;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

void CheckValuePath(TYPathBuf valuePath)
{
    TTokenizer tokenizer(valuePath);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::EndOfStream);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TCrossClusterReplicatedState
    : public ICrossClusterReplicatedState
{
public:
    TCrossClusterReplicatedState(
        IMultiClusterClientPtr client,
        ICrossClusterReplicaLockWaiterPtr lockWaiter,
        TCrossClusterReplicatedStateConfigPtr config,
        TLogger logger)
        : Client_(std::move(client))
        , LockWaiter_(std::move(lockWaiter))
        , Config_(std::move(config))
        , Logger(std::move(logger))
    {
        PathPrefixes_.reserve(Config_->Replicas.size());
        for (const auto& replicaConfig : Config_->Replicas) {
            PathPrefixes_.push_back(replicaConfig->StateDirectory);
        }
    }

    TFuture<void> ValidateStateDirectories() override
    {
        auto validateState = [this, this_ = MakeStrong(this)] (const ISingleClusterClientPtr& client) {
            auto index = client->GetIndex();
            TNodeExistsOptions options;
            options.Timeout = Config_->RequestTimeout;
            return client->GetClient()->NodeExists(PathPrefixes_[index], options)
                .Apply(BIND([this, this_ = MakeStrong(this), client, index] (bool exists) {
                    if (!exists) {
                        THROW_ERROR_EXCEPTION(
                            "State directory %Qv on cluster with index %v doesn't exist",
                            PathPrefixes_[index],
                            index);
                    }

                    TGetNodeOptions options;
                    options.Timeout = Config_->RequestTimeout;
                    return client->GetClient()->GetNode(PathPrefixes_[index] + "/@type", options);
                }))
                .AsUnique()
                .Apply(BIND([this, this_ = MakeStrong(this), index] (NYson::TYsonString&& stateDirectoryType) {
                    if (ConvertTo<TString>(stateDirectoryType) != "map_node") {
                        THROW_ERROR_EXCEPTION(
                            "State directory %Qv on cluster with index %v isn't a map_node",
                            PathPrefixes_[index],
                            index);
                    }
                    return std::monostate();
                }));
        };
        return Client_->ExecuteCallback("validate_state", BIND(std::move(validateState)))
            .AsUnique()
            .Apply(BIND(&TCrossClusterReplicatedState::ValidateStates, MakeStrong(this)));

    }

    TFuture<THashMap<TString, TString>> FetchVersions() override
    {
        auto listNodesWithVersions = [this, this_ = MakeStrong(this)] (const ISingleClusterClientPtr& client) {
            auto index = client->GetIndex();
            TListNodeOptions options{
                .Attributes = {ICrossClusterReplicatedValue::VersionAttributeKey}
            };
            options.Timeout = Config_->RequestTimeout;
            return client->GetClient()->ListNode(PathPrefixes_[index], options)
                .AsUnique()
                .Apply(BIND([] (NYson::TYsonString&& response) {
                    return ConvertToNode(response)->AsList();
                }));
        };
        return Client_->ExecuteCallback("fetch_versions", BIND(std::move(listNodesWithVersions)))
            .AsUnique()
            .Apply(BIND(&TCrossClusterReplicatedState::MergeVersions, MakeStrong(this)));
    }

    ICrossClusterReplicatedValuePtr Value(std::string tag, TYPath path) override
    {
        CheckValuePath(path);

        auto paths = PathPrefixes_;
        for (auto& pathPrefix : paths) {
            pathPrefix += '/';
            pathPrefix += path;
        }
        return CreateCrossClusterReplicatedValue(
            Client_, LockWaiter_, Config_, std::move(tag), std::move(paths), Logger);
    }

private:
    IMultiClusterClientPtr Client_;
    ICrossClusterReplicaLockWaiterPtr LockWaiter_;
    TCrossClusterReplicatedStateConfigPtr Config_;
    std::vector<TYPath> PathPrefixes_;
    TLogger Logger;

    THashMap<TString, TString> MergeVersions(std::vector<TErrorOr<IListNodePtr>>&& versionLists)
    {
        THashMap<TString, TString> entryVersions;

        std::vector<int> failedClusters;
        std::vector<TError> innerErrors;
        for (const auto& [index, versionList] : SEnumerate(versionLists)) {
            if (!versionList.IsOK()) {
                YT_LOG_DEBUG(versionList, "Received error (ReplicaIndex: %v)", index);
                failedClusters.emplace_back(index);
                innerErrors.emplace_back(static_cast<const TError&>(versionList));
                continue;
            }
            MergeVersionList(entryVersions, versionList.Value(), index);
        }

        if (failedClusters.size() * 2 >= versionLists.size()) {
            auto error = TError("Can't get versions from cluster quorum")
                << TErrorAttribute("failed_cluster_indices", failedClusters);
            *error.MutableInnerErrors() = std::move(innerErrors);
            THROW_ERROR std::move(error);
        }

        return entryVersions;
    }

    void MergeVersionList(
        THashMap<TString, TString>& entryVersions, const IListNodePtr& versionList, int replicaIndex)
    {
        for (const auto& versionEntry : versionList->GetChildren()) {
            try {
                auto rawVersion = versionEntry
                    ->Attributes()
                    .FindYson(ICrossClusterReplicatedValue::VersionAttributeKey);
                if (!rawVersion) {
                    continue;
                }

                auto entry = versionEntry->AsString()->GetValue();
                auto version = ConvertTo<TString>(rawVersion);
                entryVersions[entry] = std::max(entryVersions[entry], version);
            } catch (const TErrorException& error) {
                YT_LOG_ERROR(
                    error,
                    "Invalid version attribute format (ReplicaIndex: %v, Entry: %Qv)",
                    replicaIndex,
                    NYson::ConvertToYsonString(versionEntry, NYson::EYsonFormat::Text));
            }
        }
    }

    void ValidateStates(std::vector<TErrorOr<std::monostate>>&& checkResults)
    {
        std::vector<int> failedClusters;
        std::vector<TError> innerErrors;
        for (const auto& [index, checkResult] : SEnumerate(checkResults)) {
            if (!checkResult.IsOK()) {
                YT_LOG_DEBUG(checkResult, "Invalid state directory (ReplicaIndex: %v)", index);
                failedClusters.emplace_back(index);
                innerErrors.emplace_back(static_cast<const TError&>(checkResult));
                continue;
            }
        }

        if (failedClusters.size() * 2 >= checkResults.size()) {
            auto error = TError("Invalid state directories on cluster quorum")
                << TErrorAttribute("failed_cluster_indices", failedClusters);
            *error.MutableInnerErrors() = std::move(innerErrors);
            THROW_ERROR std::move(error);
        }
    }
};

ICrossClusterReplicatedStatePtr CreateCrossClusterReplicatedState(
    IMultiClusterClientPtr client,
    ICrossClusterReplicaLockWaiterPtr lockWaiter,
    TCrossClusterReplicatedStateConfigPtr config,
    TLogger logger)
{
    return New<TCrossClusterReplicatedState>(
        std::move(client),
        std::move(lockWaiter),
        std::move(config),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrossClusterReplicatedState
