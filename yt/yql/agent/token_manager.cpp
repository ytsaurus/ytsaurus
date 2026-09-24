#include "token_manager.h"

#include "config.h"
#include "private.h"
#include "query_identity.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/options.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/client/api/security_client.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/ytree/attributes.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NYqlAgent {

using namespace NApi;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TokenManagerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TClusterToken
{
    NApi::NNative::IClientPtr Client;
    TString Token;
    TFuture<TString> Inflight;
};

DECLARE_REFCOUNTED_STRUCT(TActiveExecution)

struct TActiveExecution
    : public TRefCounted
{
    const TQueryId QueryId;
    const TExecutionId ExecutionId;
    const TString EffectiveTokenUser;
    const EYqlTokenPurpose Purpose;
    const std::optional<TString> AllowedCluster;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    bool Active = true;
    THashMap<TString, TClusterToken> ClusterTokens;

    TActiveExecution(
        TQueryId queryId,
        TExecutionId executionId,
        TString effectiveTokenUser,
        EYqlTokenPurpose purpose,
        std::optional<TString> allowedCluster)
        : QueryId(queryId)
        , ExecutionId(executionId)
        , EffectiveTokenUser(std::move(effectiveTokenUser))
        , Purpose(purpose)
        , AllowedCluster(std::move(allowedCluster))
    { }
};

DEFINE_REFCOUNTED_TYPE(TActiveExecution)

IAttributeDictionaryPtr CreateTokenAttributes(const TActiveExecutionPtr& execution)
{
    auto attributes = CreateEphemeralAttributes();
    attributes->Set("query_id", execution->QueryId);
    attributes->Set("execution_id", execution->ExecutionId);
    attributes->Set("responsible", "yql_agent");
    return attributes;
}

void ValidateClusterForExecution(
    const TActiveExecutionPtr& execution,
    const TString& cluster)
{
    if (cluster.empty()) {
        THROW_ERROR_EXCEPTION("Cluster must not be empty");
    }

    if (execution->AllowedCluster && *execution->AllowedCluster != cluster) {
        THROW_ERROR_EXCEPTION(
            "Cluster %Qv is not allowed for query token",
            cluster)
            .With("query_id", execution->QueryId)
            .With("execution_id", execution->ExecutionId);
    }
}

class TTokenManager
    : public ITokenManager
{
public:
    TTokenManager(
        TClusterDirectoryPtr clusterDirectory,
        IInvokerPtr refreshInvoker,
        TYqlAgentConfigPtr config)
        : ClusterDirectory_(std::move(clusterDirectory))
        , Config_(std::move(config))
        , RefreshExecutor_(New<TPeriodicExecutor>(
            std::move(refreshInvoker),
            BIND(&TTokenManager::RefreshTokens, MakeWeak(this)),
            Config_->RefreshTokenPeriod))
    { }

    void Start() override
    {
        RefreshExecutor_->Start();
    }

    TFuture<void> Stop() override
    {
        return RefreshExecutor_->Stop();
    }

    TQueryIdentity RegisterQuery(
        TQueryId queryId,
        TString effectiveTokenUser,
        EYqlTokenPurpose purpose,
        std::optional<TString> allowedCluster) override
    {
        auto executionId = TExecutionId::Create();
        auto execution = New<TActiveExecution>(
            queryId,
            executionId,
            std::move(effectiveTokenUser),
            purpose,
            std::move(allowedCluster));

        auto serializedToken = QueryIdentityAuthority_.IssueToken(execution->ExecutionId);

        {
            auto guard = WriterGuard(ExecutionsLock_);
            auto [_, inserted] = Executions_.emplace(executionId, execution);
            YT_VERIFY(inserted);
        }

        return TQueryIdentity{
            .ExecutionId = executionId,
            .Token = std::move(serializedToken),
        };
    }

    void UnregisterQuery(TExecutionId executionId) override
    {
        TActiveExecutionPtr execution;
        {
            auto guard = WriterGuard(ExecutionsLock_);
            auto it = Executions_.find(executionId);
            if (it == Executions_.end()) {
                return;
            }
            execution = std::move(it->second);
            Executions_.erase(it);
        }

        auto guard = Guard(execution->Lock);
        execution->Active = false;
    }

    TFuture<TString> GetOrIssueToken(
        TExecutionId executionId,
        const TString& cluster) override
    {
        try {
            return DoGetOrIssueToken(FindExecution(executionId), cluster);
        } catch (const std::exception& ex) {
            return MakeFuture<TString>(TError(ex));
        }
    }

    TFuture<TString> IssueQueryTemporaryToken(
        const TString& queryIdentityToken,
        const TString& cluster) override
    {
        try {
            return DoGetOrIssueToken(ValidateQueryIdentity(queryIdentityToken), cluster);
        } catch (const std::exception& ex) {
            return MakeFuture<TString>(TError(ex));
        }
    }

    TString IssueTokenForClusters(
        TExecutionId executionId,
        const std::vector<TString>& clusters) override
    {
        return DoIssueTokenForClusters(FindExecution(executionId), clusters);
    }

private:
    const TClusterDirectoryPtr ClusterDirectory_;
    const TYqlAgentConfigPtr Config_;
    const TQueryIdentityAuthority QueryIdentityAuthority_;
    const TPeriodicExecutorPtr RefreshExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ExecutionsLock_);
    THashMap<TExecutionId, TActiveExecutionPtr> Executions_;

    TString DoIssueTokenForClusters(
        const TActiveExecutionPtr& execution,
        const std::vector<TString>& clusters)
    {
        for (const auto& cluster : clusters) {
            ValidateClusterForExecution(execution, cluster);
        }

        if (clusters.empty()) {
            return {};
        }

        {
            auto guard = Guard(execution->Lock);
            if (!execution->Active) {
                THROW_ERROR_EXCEPTION("Query execution is no longer active")
                    .With("query_id", execution->QueryId)
                    .With("execution_id", execution->ExecutionId);
            }
            if (!execution->ClusterTokens.empty()) {
                THROW_ERROR_EXCEPTION("Tokens have already been requested for query")
                    .With("query_id", execution->QueryId)
                    .With("execution_id", execution->ExecutionId);
            }
        }

        THashMap<TString, NApi::NNative::IClientPtr> clients;
        for (const auto& cluster : clusters) {
            clients.emplace(
                cluster,
                ClusterDirectory_
                    ->GetConnectionOrThrow(cluster)
                    ->CreateNativeClient(NApi::NNative::TClientOptions::FromUser(
                        execution->EffectiveTokenUser)));
        }

        auto attributes = CreateTokenAttributes(execution);

        auto options = TIssueTemporaryTokenOptions{
            .ExpirationTimeout = Config_->TokenExpirationTimeout,
        };

        TString token;
        bool issued = false;
        for (int attempt = 0; attempt < Config_->IssueTokenAttempts; ++attempt) {
            token.clear();
            bool retry = false;
            for (const auto& cluster : clusters) {
                auto resultOrError = token.empty()
                    ? WaitFor(GetOrCrash(clients, cluster)->IssueTemporaryToken(
                        execution->EffectiveTokenUser,
                        attributes,
                        options))
                    : WaitFor(GetOrCrash(clients, cluster)->IssueSpecificTemporaryToken(
                        execution->EffectiveTokenUser,
                        token,
                        attributes,
                        options));

                if (!resultOrError.IsOK()) {
                    YT_TLOG_WARNING("Failed to issue temporary token for query")
                        .With("QueryId", execution->QueryId)
                        .With("ExecutionId", execution->ExecutionId)
                        .With("Cluster", cluster)
                        .With("TokenUser", execution->EffectiveTokenUser)
                        .With("Attempt", attempt + 1)
                        .With(resultOrError);
                    if (resultOrError.FindMatching(NYTree::EErrorCode::AlreadyExists)) {
                        retry = true;
                        break;
                    }
                    resultOrError.ThrowOnError();
                }

                if (token.empty()) {
                    token = resultOrError.ValueOrThrow().Token;
                }
            }

            if (!retry) {
                issued = true;
                break;
            }
        }

        if (!issued) {
            THROW_ERROR_EXCEPTION("Token cannot be issued, all attempts failed")
                .With("query_id", execution->QueryId)
                .With("execution_id", execution->ExecutionId)
                .With("clusters", clusters)
                .With("token_user", execution->EffectiveTokenUser);
        }

        {
            auto guard = Guard(execution->Lock);
            if (!execution->Active) {
                THROW_ERROR_EXCEPTION("Query execution finished while issuing token")
                    .With("query_id", execution->QueryId)
                    .With("execution_id", execution->ExecutionId);
            }
            for (const auto& cluster : clusters) {
                auto& clusterToken = execution->ClusterTokens[cluster];
                clusterToken.Client = GetOrCrash(clients, cluster);
                clusterToken.Token = token;
            }
        }

        YT_TLOG_INFO("Issued temporary token for query")
            .With("QueryId", execution->QueryId)
            .With("ExecutionId", execution->ExecutionId)
            .With("Clusters", clusters)
            .With("Purpose", execution->Purpose)
            .With("TokenUser", execution->EffectiveTokenUser);

        return token;
    }

    TFuture<TString> DoGetOrIssueToken(
        const TActiveExecutionPtr& execution,
        const TString& cluster)
    {
        ValidateClusterForExecution(execution, cluster);

        TPromise<TString> issuePromise;
        TFuture<TString> inflight;
        {
            auto guard = Guard(execution->Lock);
            if (!execution->Active) {
                THROW_ERROR_EXCEPTION("Query execution is no longer active")
                    .With("query_id", execution->QueryId)
                    .With("execution_id", execution->ExecutionId);
            }
            auto& clusterToken = execution->ClusterTokens[cluster];
            if (!clusterToken.Token.empty()) {
                return MakeFuture(TString(clusterToken.Token));
            }
            if (clusterToken.Inflight) {
                // Token issuance is shared by all concurrent callers for this cluster.
                // Canceling one caller must not cancel it for the remaining callers.
                return clusterToken.Inflight.ToImmediatelyCancelable(
                    /*propagateCancelation*/ false);
            }

            issuePromise = NewPromise<TString>();
            inflight = issuePromise.ToFuture();
            clusterToken.Inflight = inflight;
        }

        auto cleanupInflight = [execution, cluster, inflight] {
            auto guard = Guard(execution->Lock);
            auto it = execution->ClusterTokens.find(cluster);
            if (it != execution->ClusterTokens.end() && it->second.Inflight == inflight) {
                execution->ClusterTokens.erase(it);
            }
        };

        auto failIssue = [cleanupInflight, issuePromise] (const TError& error) {
            cleanupInflight();
            issuePromise.TrySet(error);
        };

        try {
            auto client = ClusterDirectory_
                ->GetConnectionOrThrow(cluster)
                ->CreateNativeClient(NApi::NNative::TClientOptions::FromUser(
                    execution->EffectiveTokenUser));

            auto attributes = CreateTokenAttributes(execution);

            auto issueResultFuture = client->IssueTemporaryToken(
                execution->EffectiveTokenUser,
                attributes,
                TIssueTemporaryTokenOptions{
                    .ExpirationTimeout = Config_->TokenExpirationTimeout,
                });

            auto tokenFuture = issueResultFuture.Apply(BIND([
                execution,
                cluster,
                client = std::move(client)
            ] (const TErrorOr<TIssueTokenResult>& resultOrError) mutable -> TErrorOr<TString> {
                if (!resultOrError.IsOK()) {
                    return TError("Failed to issue temporary query token")
                        .With("query_id", execution->QueryId)
                        .With("execution_id", execution->ExecutionId)
                        .With("cluster", cluster)
                        .With(resultOrError);
                }

                auto token = TString(resultOrError.Value().Token);
                {
                    auto guard = Guard(execution->Lock);
                    if (!execution->Active) {
                        return TError("Query execution finished while issuing token")
                            .With("query_id", execution->QueryId)
                            .With("execution_id", execution->ExecutionId);
                    }

                    auto it = execution->ClusterTokens.find(cluster);
                    YT_VERIFY(it != execution->ClusterTokens.end());
                    auto& clusterToken = it->second;
                    clusterToken.Client = std::move(client);
                    clusterToken.Token = token;
                    clusterToken.Inflight = {};
                }

                YT_TLOG_INFO("Issued temporary token for query")
                    .With("QueryId", execution->QueryId)
                    .With("ExecutionId", execution->ExecutionId)
                    .With("Cluster", cluster)
                    .With("Purpose", execution->Purpose)
                    .With("TokenUser", execution->EffectiveTokenUser);

                return token;
            }));

            auto cleanupFuture = tokenFuture.Apply(BIND([cleanupInflight] (
                const TErrorOr<TString>& tokenOrError) -> TErrorOr<TString>
            {
                if (!tokenOrError.IsOK()) {
                    cleanupInflight();
                }
                return tokenOrError;
            }));

            issuePromise.SetFrom(cleanupFuture);
        } catch (const std::exception& ex) {
            auto error = TError("Failed to issue temporary query token")
                .With("query_id", execution->QueryId)
                .With("execution_id", execution->ExecutionId)
                .With("cluster", cluster)
                .With(ex);
            failIssue(error);
        } catch (...) {
            auto error = TError("Failed to issue temporary query token")
                .With("query_id", execution->QueryId)
                .With("execution_id", execution->ExecutionId)
                .With("cluster", cluster);
            failIssue(error);
            throw;
        }

        return inflight.ToImmediatelyCancelable(/*propagateCancelation*/ false);
    }

    TActiveExecutionPtr FindExecution(TExecutionId executionId) const
    {
        auto guard = ReaderGuard(ExecutionsLock_);
        auto it = Executions_.find(executionId);
        if (it == Executions_.end()) {
            THROW_ERROR_EXCEPTION("Query execution is no longer active")
                .With("execution_id", executionId);
        }
        return it->second;
    }

    TActiveExecutionPtr ValidateQueryIdentity(
        const TString& serializedToken) const
    {
        return FindExecution(QueryIdentityAuthority_.ValidateToken(serializedToken));
    }

    void RefreshTokens()
    {
        THashMap<TExecutionId, TActiveExecutionPtr> executions;
        {
            auto guard = ReaderGuard(ExecutionsLock_);
            executions = Executions_;
        }

        std::vector<TFuture<void>> futures;
        for (const auto& [_, execution] : executions) {
            THashMap<TString, TClusterToken> clusterTokens;
            {
                auto guard = Guard(execution->Lock);
                if (!execution->Active) {
                    continue;
                }
                clusterTokens = execution->ClusterTokens;
            }

            for (const auto& [cluster, clusterToken] : clusterTokens) {
                if (clusterToken.Token.empty()) {
                    continue;
                }
                futures.push_back(clusterToken.Client->RefreshTemporaryToken(
                    execution->EffectiveTokenUser,
                    clusterToken.Token,
                    /*options*/ {})
                    .Apply(BIND([
                        queryId = execution->QueryId,
                        executionId = execution->ExecutionId,
                        cluster
                    ] (const TError& error) {
                        if (!error.IsOK()) {
                            return TError(error)
                                .With("query_id", queryId)
                                .With("execution_id", executionId)
                                .With("cluster", cluster);
                        }
                        return TError{};
                    })));
            }
        }

        AllSet(std::move(futures)).Subscribe(BIND([] (const TErrorOr<std::vector<TError>>& errors) {
            if (!errors.IsOK()) {
                YT_TLOG_WARNING("Failed to refresh temporary query token")
                    .With(errors);
                return;
            }

            for (const auto& error : errors.Value()) {
                if (!error.IsOK()) {
                    YT_TLOG_WARNING("Failed to refresh temporary query token")
                        .With(error);
                }
            }
        }));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ITokenManagerPtr CreateTokenManager(
    TClusterDirectoryPtr clusterDirectory,
    IInvokerPtr refreshInvoker,
    TYqlAgentConfigPtr config)
{
    return New<TTokenManager>(
        std::move(clusterDirectory),
        std::move(refreshInvoker),
        std::move(config));
}

} // namespace NYT::NYqlAgent
