#pragma once

#include "public.h"

#include "client_factory.h"

#include <yt/yt/flow/library/cpp/common/resource.h>

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/federated_topic/federated_topic.h>
#include <ydb-cpp-sdk/client/topic/client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Owns an SDK driver and creates clients bound to it; shared by the YDB driver resources.
class TYdbDriverCore
    : public TRefCounted
{
public:
    TYdbDriverCore(NYdb::TDriverConfig config, bool stopBeforeDestroying);
    ~TYdbDriverCore() override;

    std::shared_ptr<NYdb::NTopic::TTopicClient> CreateTopicClient(
        const std::string& endpoint,
        const std::string& database);

    std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> CreateFederatedTopicClient(
        const std::string& endpoint,
        const std::string& database);

    //! The writer keeps |owner| alive.
    IYdbWriterPtr CreateWriter(
        IResourcePtr owner,
        const std::string& endpoint,
        const std::string& database,
        NYdb::NTopic::TWriteSessionSettings settings,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler);

    //! The client keeps |owner| alive.
    IDeferredPublicationClientPtr CreateDeferredPublicationClient(
        IResourcePtr owner,
        const std::string& endpoint,
        const std::string& database);

    TFuture<TYdbWriteTargetResolution> ResolveWriteTarget(
        const TYdbTopologyOptions& options,
        TDuration timeout,
        IInvokerPtr invoker,
        std::optional<TYdbWriteTarget> preferredTarget,
        THashSet<TYdbWriteTarget, TYdbWriteTargetHash> excludedTargets);

private:
    NYdb::TDriver Driver_;
    const bool StopBeforeDestroying_;
};

DEFINE_REFCOUNTED_TYPE(TYdbDriverCore);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TYdbWriteTargetResolution SelectYdbWriteTarget(
    const std::vector<NYdb::NFederatedTopic::TFederatedTopicClient::TClusterInfo>& clusters,
    const TYdbTopologyOptions& options,
    const std::optional<TYdbWriteTarget>& preferredTarget = std::nullopt,
    const THashSet<TYdbWriteTarget, TYdbWriteTargetHash>& excludedTargets = {});

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
