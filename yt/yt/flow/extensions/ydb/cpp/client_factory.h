#pragma once

#include "public.h"

#include "options.h"

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/logging/log.h>

#include <util/generic/hash_set.h>

#include <ydb-cpp-sdk/client/federated_topic/federated_topic.h>
#include <ydb-cpp-sdk/client/topic/client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IYdbClientFactory
    : public virtual TRefCounted
{
    virtual std::shared_ptr<NYdb::NTopic::TTopicClient> CreateTopicClient(
        const std::string& endpoint,
        const std::string& database) = 0;

    //! Deliberately not cached: a fresh client's observer performs a live discovery request, while a
    //! shared one serves possibly stale (or, after the SDK's single-cluster fallback, permanently wrong)
    //! cached state and hides discovery outages.
    virtual std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> CreateFederatedTopicClient(
        const std::string& endpoint,
        const std::string& database) = 0;

    //! Creates an independent writer running on |invoker| and owned by the caller.
    virtual IYdbWriterPtr CreateWriter(
        const std::string& endpoint,
        const std::string& database,
        NYdb::NTopic::TWriteSessionSettings settings,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler) = 0;

    virtual IDeferredPublicationClientPtr CreateDeferredPublicationClient(
        const std::string& endpoint,
        const std::string& database) = 0;

    virtual TFuture<TYdbWriteTargetResolution> ResolveWriteTarget(
        const TYdbTopologyOptions& options,
        TDuration timeout,
        std::optional<TYdbWriteTarget> preferredTarget,
        THashSet<TYdbWriteTarget, TYdbWriteTargetHash> excludedTargets) = 0;
};

DEFINE_REFCOUNTED_TYPE(IYdbClientFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
