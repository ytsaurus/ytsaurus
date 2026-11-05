#include "helpers.h"

#include "connection.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/auth/native_authenticator.h>
#include <yt/yt/ytlib/auth/native_authentication_manager.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>
#include <yt/yt/ytlib/security_client/user_attribute_cache.h>

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NNative {

const auto& Logger = NativeConnectionLogger;

////////////////////////////////////////////////////////////////////////////////

using namespace NAuth;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NScheduler;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

TError MakeOperationRevivalError()
{
    return TError("Operation of job is reviving");
}

TAllocationBriefInfo ParseGetBreifAllocationInfoResponse(
    TAllocationInfoToRequest allocationInfoToRequest,
    TAllocationId allocationId,
    const TOperationServiceProxy::TErrorOrRspGetAllocationBriefInfoPtr& rspOrError)
{
    if (!rspOrError.IsOK()) {
        if (NApi::NNative::IsRevivalError(rspOrError)) {
            THROW_ERROR MakeOperationRevivalError();
        }

        THROW_ERROR(rspOrError);
    }

    const auto& rsp = rspOrError.Value();

    TAllocationBriefInfo result;
    FromProto(&result, rsp->allocation_brief_info());

    YT_VERIFY(allocationId == result.AllocationId);

    if (allocationInfoToRequest.OperationId) {
        YT_TLOG_FATAL_UNLESS(result.OperationId, "Operation id is missing in scheduler response")
            .With("AllocationId", allocationId);
    }

    if (allocationInfoToRequest.OperationAcl) {
        YT_TLOG_FATAL_UNLESS(result.OperationAcl, "Operation acl is missing in scheduler response")
            .With("AllocationId", allocationId);
    }

    if (allocationInfoToRequest.ControllerAgentDescriptor) {
        YT_TLOG_FATAL_UNLESS(
            result.ControllerAgentDescriptor,
            "Controller agent descriptor is missing in scheduler response")
            .With("AllocationId", allocationId);
        YT_TLOG_FATAL_UNLESS(
            result.ControllerAgentDescriptor.Addresses,
            "Controller agent addresses is missing in scheduler response")
            .With("AllocationId", allocationId)
            .With("ControllerAgentDescriptor", result.ControllerAgentDescriptor);
    }

    if (allocationInfoToRequest.NodeDescriptor) {
        YT_TLOG_FATAL_IF(result.NodeDescriptor.IsNull(), "Node descriptor is missing in scheduler response")
            .With("AllocationId", allocationId);
    }

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TError ValidateSourceTvmId(const IConnectionPtr& connection, TTvmId tvmId)
{
    if (tvmId == connection->GetConfig()->TvmId) {
        return {};
    }

    const auto& clusterDirectory = connection->GetClusterDirectory();

    // NB: lastUpdateTime is set after populating clusterDirectory, read it before probing tvmId.
    auto lastUpdateTime = clusterDirectory->GetLastSuccessfulUpdateTime();
    if (clusterDirectory->HasTvmId(tvmId)) {
        return {};
    }

    const auto& synchronizerConfig = connection->GetConfig()->ClusterDirectorySynchronizer;
    auto maxStaleness = synchronizerConfig->SyncPeriod * synchronizerConfig->TvmIdRejectionStalenessMultiplier;
    if (!lastUpdateTime || TInstant::Now() - *lastUpdateTime > maxStaleness) {
        return TError(
            NRpc::EErrorCode::TransientFailure,
            "Cannot validate source TVM id %v since cluster directory has not been synchronized recently",
            tvmId)
            .With("last_successful_update_time", lastUpdateTime);
    }

    return TError(
        NRpc::EErrorCode::AuthenticationError,
        "Source TVM id %v is rejected",
        tvmId);
}

IAuthenticatorPtr CreateNativeAuthenticator(const IConnectionPtr& connection)
{
    return NAuth::CreateNativeAuthenticator([connection] (TTvmId tvmId) {
        return ValidateSourceTvmId(connection, tvmId);
    });
}

////////////////////////////////////////////////////////////////////////////////

void SetupClusterConnectionDynamicConfigUpdate(
    const IConnectionPtr& connection,
    EClusterConnectionDynamicConfigPolicy policy,
    const INodePtr& staticClusterConnectionNode,
    const TLogger logger)
{
    auto Logger = logger;
    if (policy == EClusterConnectionDynamicConfigPolicy::FromStaticConfig) {
        return;
    }

    YT_TLOG_INFO("Setting up cluster connection dynamic config update")
        .With("Policy", policy)
        .With("Cluster", connection->GetClusterName());

    connection->GetClusterDirectory()->SubscribeOnClusterUpdated(BIND([=] (const std::string& clusterName, const INodePtr& configNode) {
        if (clusterName != connection->GetClusterName()) {
            YT_TLOG_DEBUG("Skipping cluster directory update for unrelated cluster")
                .With("UpdatedCluster", clusterName);
            return;
        }

        auto dynamicConfigNode = configNode;

        YT_TLOG_DEBUG("Applying cluster connection update from cluster directory")
            .With("DynamicConfig", ConvertToYsonString(dynamicConfigNode, EYsonFormat::Text).ToString());

        if (policy == EClusterConnectionDynamicConfigPolicy::FromClusterDirectoryWithStaticPatch) {
            dynamicConfigNode = PatchNode(dynamicConfigNode, staticClusterConnectionNode);
            YT_TLOG_DEBUG("Patching cluster connection dynamic config with static config")
                .With("DynamicConfig", ConvertToYsonString(dynamicConfigNode, EYsonFormat::Text).ToString());
        }

        TConnectionDynamicConfigPtr dynamicConfig;
        try {
            dynamicConfig = ConvertTo<TConnectionDynamicConfigPtr>(dynamicConfigNode);
            connection->Reconfigure(dynamicConfig);

            YT_TLOG_DEBUG("Cluster connection dynamic config applied")
                .With("Policy", policy)
                .With("Cluster", connection->GetClusterName())
                .With("DynamicConfig", ConvertToYsonString(dynamicConfigNode, EYsonFormat::Text).ToString());
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Failed to apply cluster connection dynamic config, ignoring update")
                .With(ex);
            return;
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TAllocationBriefInfo> GetAllocationBriefInfo(
    const NScheduler::TOperationServiceProxy& operationServiceProxy,
    NScheduler::TAllocationId allocationId,
    TAllocationInfoToRequest allocationInfoToRequest)
{
    auto req = operationServiceProxy.GetAllocationBriefInfo();

    ToProto(req->mutable_allocation_id(), allocationId);

    auto* infoToRequest = req->mutable_requested_info();

    ToProto(infoToRequest, allocationInfoToRequest);

    return req->Invoke().Apply(BIND(&ParseGetBreifAllocationInfoResponse, allocationInfoToRequest, allocationId));
}

////////////////////////////////////////////////////////////////////////////////

bool IsRevivalError(const TError& error)
{
    return error.FindMatching(NControllerAgent::EErrorCode::AgentDisconnected) ||
        error.FindMatching(NControllerAgent::EErrorCode::IncarnationMismatch) ||
        error.FindMatching(NScheduler::EErrorCode::AgentRevoked);
}

TError MakeRevivalError(
    NScheduler::TOperationId operationId,
    NScheduler::TJobId jobId)
{
    return MakeOperationRevivalError()
        .With("job_id", jobId)
        .With("operation_id", operationId);
}

////////////////////////////////////////////////////////////////////////////////

void CheckPermission(
    const NYPath::TYPath& path,
    const TTableMountInfoPtr& tableInfo,
    const TAuthenticationOptions& options,
    const IConnectionPtr& connection,
    EPermission permission)
{
    NSecurityClient::TPermissionKey permissionKey{
        .Path = FromObjectId(tableInfo->TableId),
        .User = options.GetAuthenticatedUser(),
        .Permission = permission,
    };
    const auto& permissionCache = connection->GetPermissionCache();
    WaitFor(permissionCache->Get(permissionKey))
        .ThrowOnError("No %v permission for %v", permission, path);
}

void CheckReadPermission(
    const NYPath::TYPath& path,
    const TTableMountInfoPtr& tableInfo,
    const TAuthenticationOptions& options,
    const IConnectionPtr& connection)
{
    CheckPermission(path, tableInfo, options, connection, EPermission::Read);
}

void CheckWritePermission(
    const NYPath::TYPath& path,
    const TTableMountInfoPtr& tableInfo,
    const TAuthenticationOptions& options,
    const IConnectionPtr& connection)
{
    CheckPermission(path, tableInfo, options, connection, EPermission::Write);
}

////////////////////////////////////////////////////////////////////////////////

THashSet<std::string> DeduceActualAttributes(
    const std::optional<THashSet<std::string>>& originalAttributes,
    const THashSet<std::string>& requiredAttributes,
    const THashSet<std::string>& defaultAttributes,
    const THashSet<std::string>& ignoredAttributes)
{
    auto attributes = originalAttributes.value_or(defaultAttributes);
    attributes.insert(requiredAttributes.begin(), requiredAttributes.end());
    for (const auto& attribute : ignoredAttributes) {
        attributes.erase(attribute);
    }
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

TSelectRowsOptions GetDefaultSelectRowsOptions(
    TInstant deadline,
    NTransactionClient::TTimestamp timestamp)
{
    TSelectRowsOptions selectRowsOptions;
    selectRowsOptions.Timestamp = timestamp;
    selectRowsOptions.Timeout = deadline - Now();
    selectRowsOptions.InputRowLimit = std::numeric_limits<i64>::max();
    selectRowsOptions.MemoryLimitPerNode = 100_MB;
    return selectRowsOptions;
}

////////////////////////////////////////////////////////////////////////////////

TDuration InvalidateMountCacheAndGetRetryDelay(
    const IConnectionPtr& connection,
    const TDetailedProfilingInfoPtr& profilingInfo,
    const TLogger& Logger,
    const TError& error,
    int* retryCount,
    TTabletId tabletIdHint)
{
    const auto& config = connection->GetStaticConfig();
    const auto& tableMountCache = connection->GetTableMountCache();

    auto invalidationResult = tableMountCache->InvalidateOnError(
        error,
        /*forceRetry*/ false,
        tabletIdHint);

    TDuration timeToWait;
    if (invalidationResult.Retryable && ++(*retryCount) <= config->TableMountCache->OnErrorRetryCount) {
        YT_TLOG_DEBUG("Got error, will retry")
            .With("Attempt", *retryCount)
            .With("AttemptCount", config->TableMountCache->OnErrorRetryCount)
            .With(error);

        if (!invalidationResult.TableInfoUpdatedFromError) {
            auto now = Now();
            const auto& tabletInfo = invalidationResult.TabletInfo;
            auto retryTime = (tabletInfo ? tabletInfo->UpdateTime : now) +
                config->TableMountCache->OnErrorSlackPeriod;
            if (retryTime > now) {
                timeToWait = retryTime - now;
            }
        }

        if (profilingInfo) {
            profilingInfo->RetryReasons.push_back(invalidationResult.ErrorCode);
        }

        return timeToWait;
    }

    THROW_ERROR error;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TTableMountInfoPtr> GetTableMountInfo(const TRichYPath& objectPath, const IConnectionPtr& connection)
{
    const auto& objectCluster = objectPath.GetCluster();
    // NB: For better cache locality, use the provided connection when its cluster is equal to the object's cluster.
    auto objectConnection = ((objectCluster && objectCluster == connection->GetClusterName())
        ? connection
        : FindRemoteConnection(connection, objectPath.GetCluster()));
    YT_VERIFY(objectConnection);
    auto objectTableMountCache = objectConnection->GetTableMountCache();
    return objectTableMountCache->GetTableInfo(objectPath.GetPath());
}

////////////////////////////////////////////////////////////////////////////////

TFuture<bool> IsSuperuser(const IConnectionPtr& connection, const std::string& user)
{
    return connection->GetUserAttributeCache()->Get(user)
        .Apply(BIND([] (const TUserAttributesPtr& attributes) {
            YT_VERIFY(attributes);
            return attributes->MemberOfClosure.contains(SuperusersGroupName);
        }));
}

TFuture<bool> IsUserBanned(const IConnectionPtr& connection, const std::string& user)
{
    return connection->GetUserAttributeCache()->Get(user)
        .Apply(BIND([] (const TUserAttributesPtr& attributes) {
            YT_VERIFY(attributes);
            return attributes->Banned;
        }));
}

TClientChunkReadOptions MakeChunkReadOptions(
    TReadSessionId readSessionId,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    const TTableReaderConfigPtr& tableReaderConfig,
    const TYPath& yPath)
{
    auto chunkReadOptions = TClientChunkReadOptions{
        .WorkloadDescriptor = tableReaderConfig->WorkloadDescriptor,
        .ReadSessionId = readSessionId,
        .MemoryUsageTracker = std::move(memoryUsageTracker),
    };
    if (!yPath.empty()) {
        chunkReadOptions.WorkloadDescriptor.Annotations.push_back(Format("TablePath: %v", yPath));
    }
    return chunkReadOptions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
