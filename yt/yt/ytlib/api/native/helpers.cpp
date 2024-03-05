#include "helpers.h"

#include "connection.h"
#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/auth/native_authenticator.h>
#include <yt/yt/ytlib/auth/native_authentication_manager.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/scheduler/scheduler_service_proxy.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NNative {

const auto& Logger = NativeConnectionLogger;

////////////////////////////////////////////////////////////////////////////////

using namespace NAuth;
using namespace NRpc;
using namespace NYTree;
using namespace NLogging;
using namespace NYson;
using namespace NSecurityClient;
using namespace NScheduler;
using namespace NConcurrency;

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
        YT_LOG_FATAL_UNLESS(
            result.OperationId,
            "Operation id is missing in scheduler response (AllocationId: %v)",
            allocationId);
    }

    if (allocationInfoToRequest.OperationAcl) {
        YT_LOG_FATAL_UNLESS(
            result.OperationAcl,
            "Operation acl is missing in scheduler response (AllocationId: %v)",
            allocationId);
    }

    if (allocationInfoToRequest.ControllerAgentDescriptor) {
        YT_LOG_FATAL_UNLESS(
            result.ControllerAgentDescriptor,
            "Controller agent descriptor is missing in scheduler response (AllocationId: %v)",
            allocationId);
        YT_LOG_FATAL_UNLESS(
            result.ControllerAgentDescriptor.Addresses,
            "Controller agent addresses is missing in scheduler response (AllocationId: %v, ControllerAgentDescriptor: %v))",
            allocationId,
            result.ControllerAgentDescriptor);
    }

    if (allocationInfoToRequest.NodeDescriptor) {
        YT_LOG_FATAL_IF(
            result.NodeDescriptor.IsNull(),
            "Node descriptor is missng in scheduler response (AllocationId: %v)",
            allocationId);
    }

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsValidSourceTvmId(const IConnectionPtr& connection, TTvmId tvmId)
{
    return tvmId == connection->GetConfig()->TvmId || connection->GetClusterDirectory()->HasTvmId(tvmId);
}

IAuthenticatorPtr CreateNativeAuthenticator(const IConnectionPtr& connection)
{
    return NAuth::CreateNativeAuthenticator([connection] (TTvmId tvmId) {
        return IsValidSourceTvmId(connection, tvmId);
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

    YT_LOG_INFO(
        "Setting up cluster connection dynamic config update (Policy: %v, Cluster: %v)",
        policy,
        connection->GetClusterName());

    connection->GetClusterDirectory()->SubscribeOnClusterUpdated(BIND([=] (const TString& clusterName, const INodePtr& configNode) {
        if (clusterName != connection->GetClusterName()) {
            YT_LOG_DEBUG(
                "Skipping cluster directory update for unrelated cluster (UpdatedCluster: %v)",
                clusterName);
            return;
        }

        auto dynamicConfigNode = configNode;

        YT_LOG_DEBUG(
            "Applying cluster connection update from cluster directory (DynamicConfig: %v)",
            ConvertToYsonString(dynamicConfigNode, EYsonFormat::Text).ToString());

        if (policy == EClusterConnectionDynamicConfigPolicy::FromClusterDirectoryWithStaticPatch) {
            dynamicConfigNode = PatchNode(dynamicConfigNode, staticClusterConnectionNode);
            YT_LOG_DEBUG(
                "Patching cluster connection dynamic config with static config (DynamicConfig: %v)",
                ConvertToYsonString(dynamicConfigNode, EYsonFormat::Text).ToString());
        }

        TConnectionDynamicConfigPtr dynamicConfig;
        try {
            dynamicConfig = ConvertTo<TConnectionDynamicConfigPtr>(dynamicConfigNode);
            connection->Reconfigure(dynamicConfig);

            YT_LOG_DEBUG("Cluster connection dynamic config applied (Policy: %v, Cluster: %v, DynamicConfig: %v)",
                policy,
                connection->GetClusterName(),
                ConvertToYsonString(dynamicConfigNode, EYsonFormat::Text).ToString());
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(
                ex,
                "Failed to apply cluster connection dynamic config, ignoring update");
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
        << TErrorAttribute("job_id", jobId)
        << TErrorAttribute("operation_id", operationId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
