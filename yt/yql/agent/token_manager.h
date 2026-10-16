#pragma once

#include "public.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/string.h>

#include <optional>
#include <vector>

namespace NYT::NYqlAgent {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EYqlTokenPurpose,
    (Regular)
    (UdfMeta)
    (DeclaredParameters)
);

struct TQueryIdentity
{
    TExecutionId ExecutionId;
    TString Token;
};

struct ITokenManager
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual TFuture<void> Stop() = 0;

    virtual TQueryIdentity RegisterQuery(
        TQueryId queryId,
        TString effectiveTokenUser,
        EYqlTokenPurpose purpose,
        std::optional<TString> allowedCluster) = 0;

    virtual void UnregisterQuery(TExecutionId executionId) = 0;

    virtual TFuture<TString> GetOrIssueToken(
        TExecutionId executionId,
        const TString& cluster) = 0;

    virtual TFuture<TString> IssueQueryTemporaryToken(
        const TString& queryIdentityToken,
        const TString& cluster) = 0;

    // TODO(ziganshinmr): remove after full transition to token resolver.
    virtual TString IssueTokenForClusters(
        TExecutionId executionId,
        const std::vector<TString>& clusters) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITokenManager)

ITokenManagerPtr CreateTokenManager(
    NHiveClient::TClusterDirectoryPtr clusterDirectory,
    IInvokerPtr refreshInvoker,
    TYqlAgentConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlAgent
