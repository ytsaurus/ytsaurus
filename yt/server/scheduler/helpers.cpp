#include "helpers.h"
#include "public.h"
#include "exec_node.h"
#include "job.h"
#include "operation.h"
#include "operation_controller.h"
#include "chunk_pool.h"

#include <yt/ytlib/chunk_client/input_slice.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/ypath/rich.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////

TJobSizeLimits::TJobSizeLimits(
    i64 totalDataSize,
    i64 dataSizePerJob,
    TNullable<int> configJobCount,
    int maxJobCount)
    : TotalDataSize_(totalDataSize)
    , MaxJobCount_(maxJobCount)
{
    if (configJobCount) {
        SetJobCount(*configJobCount);
    } else {
        SetDataSizePerJob(dataSizePerJob);
    }
}

void TJobSizeLimits::SetJobCount(i64 jobCount)
{
    JobCount_ = Clamp(jobCount, 1, MaxJobCount_);
    DataSizePerJob_ = DivCeil(TotalDataSize_, JobCount_);
}

int TJobSizeLimits::GetJobCount() const
{
    return JobCount_;
}

void TJobSizeLimits::SetDataSizePerJob(i64 dataSizePerJob)
{
    SetJobCount(DivCeil(TotalDataSize_, dataSizePerJob));
}

i64 TJobSizeLimits::GetDataSizePerJob() const
{
    return DataSizePerJob_;
}

////////////////////////////////////////////////////////////////////

void BuildInitializingOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpec())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Do(BIND(&BuildRunningOperationAttributes, operation));
}

void BuildRunningOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    auto controller = operation->GetController();
    auto userTransaction = operation->GetUserTransaction();
    BuildYsonMapFluently(consumer)
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended())
        .Item("user_transaction_id").Value(userTransaction ? userTransaction->GetId() : NullTransactionId)
        .Item("events").Value(operation->GetEvents())
        .DoIf(static_cast<bool>(controller), BIND(&IOperationController::BuildOperationAttributes, controller));
}

void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer)
{
    auto state = job->GetState();
    BuildYsonMapFluently(consumer)
        .Item("job_type").Value(FormatEnum(job->GetType()))
        .Item("state").Value(FormatEnum(state))
        .Item("address").Value(job->GetNode()->GetDefaultAddress())
        .Item("start_time").Value(job->GetStartTime())
        .Item("account").Value(job->GetAccount())
        .Item("progress").Value(job->GetProgress())
        .DoIf(static_cast<bool>(job->GetBriefStatistics()), [=] (TFluentMap fluent) {
            fluent.Item("brief_statistics").Value(*job->GetBriefStatistics());
        })
        .Item("suspicious").Value(job->GetSuspicious())
        .DoIf(job->GetFinishTime().HasValue(), [=] (TFluentMap fluent) {
            fluent.Item("finish_time").Value(job->GetFinishTime().Get());
        })
        .DoIf(state == EJobState::Failed, [=] (TFluentMap fluent) {
            auto error = FromProto<TError>(job->Status().result().error());
            fluent.Item("error").Value(error);
        });
        // XXX(babenko): YT-4948
        //.DoIf(job->Status() && job->Status()->has_statistics(), [=] (TFluentMap fluent) {
        //    fluent.Item("statistics").Value(NYson::TYsonString(job->Status()->statistics()));
        //});
}

void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("state").Value(node->GetMasterState())
        .Item("resource_usage").Value(node->GetResourceUsage())
        .Item("resource_limits").Value(node->GetResourceLimits());
}

static void BuildInputSliceLimit(
    const TInputSlicePtr& slice,
    const TInputSliceLimit& limit,
    TNullable<i64> rowIndex,
    NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(limit.RowIndex.operator bool() || rowIndex, [&] (TFluentMap fluent) {
                fluent
                    .Item("row_index").Value(
                        limit.RowIndex.Get(rowIndex.Get(0)) + slice->GetInputChunk()->GetTableRowIndex());
            })
            .DoIf(limit.Key.operator bool(), [&] (TFluentMap fluent) {
                fluent
                    .Item("key").Value(limit.Key);
            })
        .EndMap();
}

TYsonString BuildInputPaths(
    const std::vector<TRichYPath>& inputPaths,
    const TChunkStripeListPtr& inputStripeList,
    EOperationType operationType,
    EJobType jobType)
{
    bool hasSlices = false;
    std::vector<std::vector<TInputSlicePtr>> slicesByTable(inputPaths.size());
    for (const auto& stripe : inputStripeList->Stripes) {
        for (const auto& slice : stripe->ChunkSlices) {
            auto tableIndex = slice->GetInputChunk()->GetTableIndex();
            if (tableIndex >= 0) {
                slicesByTable[tableIndex].push_back(slice);
                hasSlices = true;
            }
        }
    }
    if (!hasSlices) {
        return TYsonString();
    }

    std::vector<char> isForeignTable(inputPaths.size());
    std::transform(
        inputPaths.begin(),
        inputPaths.end(),
        isForeignTable.begin(),
        [](const TRichYPath& path) { return path.GetForeign(); });

    std::vector<std::vector<std::pair<TInputSlicePtr, TInputSlicePtr>>> rangesByTable(inputPaths.size());
    bool mergeByRows = !(
        operationType == EOperationType::Reduce ||
        (operationType == EOperationType::Merge && jobType == EJobType::SortedMerge));
    for (int tableIndex = 0; tableIndex < static_cast<int>(slicesByTable.size()); ++tableIndex) {
        auto& tableSlices = slicesByTable[tableIndex];

        std::sort(tableSlices.begin(), tableSlices.end(), &CompareSlicesByLowerLimit);

        int firstSlice = 0;
        while (firstSlice < static_cast<int>(tableSlices.size())) {
            int lastSlice = firstSlice + 1;
            while (lastSlice < static_cast<int>(tableSlices.size())) {
                if (mergeByRows && !isForeignTable[tableIndex] &&
                    !CanMergeSlices(tableSlices[lastSlice - 1], tableSlices[lastSlice]))
                {
                    break;
                }
                ++lastSlice;
            }
            rangesByTable[tableIndex].emplace_back(tableSlices[firstSlice], tableSlices[lastSlice - 1]);
            firstSlice = lastSlice;
        }
    }

    return BuildYsonStringFluently()
        .DoListFor(rangesByTable, [&] (TFluentList fluent, const std::vector<std::pair<TInputSlicePtr, TInputSlicePtr>>& tableRanges) {
            fluent
                .DoIf(!tableRanges.empty(), [&] (TFluentList fluent) {
                    int tableIndex = tableRanges[0].first->GetInputChunk()->GetTableIndex();
                    fluent
                        .Item()
                        .BeginAttributes()
                            .DoIf(isForeignTable[tableIndex], [&] (TFluentAttributes fluent) {
                                fluent
                                    .Item("foreign").Value(true);
                            })
                            .Item("ranges")
                            .DoListFor(tableRanges, [&] (TFluentList fluent, const std::pair<TInputSlicePtr, TInputSlicePtr>& range) {
                                fluent
                                    .Item()
                                    .BeginMap()
                                        .Item("lower_limit")
                                        .Do(BIND(
                                            &BuildInputSliceLimit,
                                            range.first,
                                            range.first->LowerLimit(),
                                            TNullable<i64>(mergeByRows && !isForeignTable[tableIndex], 0)))
                                        .Item("upper_limit")
                                        .Do(BIND(
                                            &BuildInputSliceLimit,
                                            range.second,
                                            range.second->UpperLimit(),
                                            TNullable<i64>(mergeByRows && !isForeignTable[tableIndex], range.second->GetInputChunk()->GetRowCount())))
                                    .EndMap();
                            })
                        .EndAttributes()
                        .Value(inputPaths[tableIndex].GetPath());
                });
        });
}

////////////////////////////////////////////////////////////////////////////////

i64 Clamp(i64 value, i64 minValue, i64 maxValue)
{
    value = std::min(value, maxValue);
    value = std::max(value, minValue);
    return value;
}

i64 DivCeil(i64 numerator, i64 denominator)
{
    auto res = std::div(numerator, denominator);
    return res.quot + (res.rem > 0 ? 1 : 0);
}

Stroka TrimCommandForBriefSpec(const Stroka& command)
{
    const int MaxBriefSpecCommandLength = 256;
    return
        command.length() <= MaxBriefSpecCommandLength
        ? command
        : command.substr(0, MaxBriefSpecCommandLength) + "...";
}

////////////////////////////////////////////////////////////////////

EAbortReason GetAbortReason(const NJobTrackerClient::NProto::TJobResult& result)
{
    auto error = FromProto<TError>(result.error());
    try {
        return error.Attributes().Get<EAbortReason>("abort_reason", EAbortReason::Scheduler);
    } catch (const std::exception& ex) {
        // Process unknown abort reason from node.
        LOG_WARNING(ex, "Found unknown abort_reason in job result");
        return EAbortReason::Unknown;
    }
}

////////////////////////////////////////////////////////////////////

Stroka MakeOperationCodicilString(const TOperationId& operationId)
{
    return Format("OperationId: %v", operationId);
}

TCodicilGuard MakeOperationCodicilGuard(const TOperationId& operationId)
{
    return TCodicilGuard(MakeOperationCodicilString(operationId));
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

