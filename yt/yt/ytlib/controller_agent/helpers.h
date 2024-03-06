#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void Serialize(const TCoreInfo& coreInfo, NYson::IYsonConsumer* consumer);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TJobFile
{
    TJobId JobId;
    NYPath::TYPath Path;
    NChunkClient::TChunkId ChunkId;
    TString DescriptionType;
};

void SaveJobFiles(
    const NApi::NNative::IClientPtr& client,
    TOperationId operationId,
    const std::vector<TJobFile>& files,
    NTransactionClient::TTransactionId transactionId);

////////////////////////////////////////////////////////////////////////////////

int GetJobSpecVersion();

bool IsOperationWithUserJobs(EOperationType operationType);

void ValidateEnvironmentVariableName(TStringBuf name);

bool WasAbortedAfterStart(EAbortReason reason);

bool IsFinishedState(EControllerState state);

// Used in node and client.
NYson::TYsonString BuildBriefStatistics(const NYTree::INodePtr& statistics);

////////////////////////////////////////////////////////////////////////////////

void SanitizeJobSpec(NProto::TJobSpec* jobSpec);

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TControllerAgentDescriptor* controllerAgentDescriptor,
    const NProto::TControllerAgentDescriptor& controllerAgentDescriptorProto);
void ToProto(
    NProto::TControllerAgentDescriptor* controllerAgentDescriptorProto,
    const TControllerAgentDescriptor& controllerAgentDescriptor);

void FormatValue(
    TStringBuilderBase* builder,
    const TControllerAgentDescriptor& controllerAgentDescriptor,
    TStringBuf /*format*/);

////////////////////////////////////////////////////////////////////////////////

bool AreCompatible(ELayerAccessMethod accessMethod, ELayerFilesystem filesystem);

////////////////////////////////////////////////////////////////////////////////

void AdvanceEpoch(TControllerEpoch& epoch);

////////////////////////////////////////////////////////////////////////////////

TIncarnationId IncarnationIdFromTransactionId(NObjectClient::TTransactionId transactionId);

NObjectClient::TTransactionId IncarnationIdToTransactionId(TIncarnationId incarnationId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
