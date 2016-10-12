#pragma once

#include "private.h"

#include <yt/ytlib/hive/cluster_directory.h>
#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJobSizeLimits
{
public:
    TJobSizeLimits(i64 totalDataSize, i64 dataSizePerJob, TNullable<int> configJobCount, int maxJobCount);
    void SetJobCount(i64 jobCount);
    int GetJobCount() const;
    void SetDataSizePerJob(i64 dataSizePerJob);
    i64 GetDataSizePerJob() const;

private:
    i64 TotalDataSize_ = 0;
    int MaxJobCount_ = 0;
    int JobCount_ = 0;
    i64 DataSizePerJob_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

void BuildInitializingOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer);
void BuildRunningOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer);
void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer);
void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer);
TNullable<NYson::TYsonString> BuildInputPaths(
    const std::vector<NYPath::TRichYPath>& inputPaths,
    const TChunkStripeListPtr& inputStripeList,
    EOperationType operationType,
    EJobType jobType);

////////////////////////////////////////////////////////////////////////////////

i64 Clamp(i64 value, i64 minValue, i64 maxValue);
i64 DivCeil(i64 numerator, i64 denominator);

Stroka TrimCommandForBriefSpec(const Stroka& command);

////////////////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::IMapNodePtr specNode);

////////////////////////////////////////////////////////////////////

EAbortReason GetAbortReason(const NJobTrackerClient::NProto::TJobResult& result);

////////////////////////////////////////////////////////////////////

Stroka MakeOperationCodicilString(const TOperationId& operationId);
TCodicilGuard MakeOperationCodicilGuard(const TOperationId& operationId);

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

////////////////////////////////////////////////////////////////////

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
