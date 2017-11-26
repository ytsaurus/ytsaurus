#pragma once

#include "private.h"

#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IJobHost
    : public TIntrinsicRefCounted
{
    virtual TFuture<void> InterruptJob(EInterruptReason reason) = 0;

    virtual TFuture<void> AbortJob(const TError& error) = 0;

    virtual TFuture<void> FailJob() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJobHost)

////////////////////////////////////////////////////////////////////////////////

void BuildInitializingOperationAttributes(TOperationPtr operation, NYTree::TFluentMap fluent);
void BuildRunningOperationAttributes(TOperationPtr operation, NYTree::TFluentMap fluent);
void BuildExecNodeAttributes(TExecNodePtr node, NYTree::TFluentMap fluent);

////////////////////////////////////////////////////////////////////////////////

EAbortReason GetAbortReason(const NJobTrackerClient::NProto::TJobResult& result);

////////////////////////////////////////////////////////////////////////////////

TString MakeOperationCodicilString(const TOperationId& operationId);
TCodicilGuard MakeOperationCodicilGuard(const TOperationId& operationId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

