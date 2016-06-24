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

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NChunkClient;

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
    auto userTransaction = operation->GetUserTransaction();
    auto syncTransaction = operation->GetSyncSchedulerTransaction();
    auto asyncTransaction = operation->GetAsyncSchedulerTransaction();
    auto inputTransaction = operation->GetInputTransaction();
    auto outputTransaction = operation->GetOutputTransaction();
    BuildYsonMapFluently(consumer)
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended())
        .Item("user_transaction_id").Value(userTransaction ? userTransaction->GetId() : NullTransactionId)
        .Item("sync_scheduler_transaction_id").Value(syncTransaction ? syncTransaction->GetId() : NullTransactionId)
        .Item("async_scheduler_transaction_id").Value(asyncTransaction ? asyncTransaction->GetId() : NullTransactionId)
        .Item("input_transaction_id").Value(inputTransaction ? inputTransaction->GetId() : NullTransactionId)
        .Item("output_transaction_id").Value(outputTransaction ? outputTransaction->GetId() : NullTransactionId);
}

void BuildJobAttributes(TJobPtr job, const TNullable<NYson::TYsonString>& inputPaths, NYson::IYsonConsumer* consumer)
{
    auto state = job->GetState();
    BuildYsonMapFluently(consumer)
        .Item("job_type").Value(FormatEnum(job->GetType()))
        .Item("state").Value(FormatEnum(state))
        .Item("address").Value(job->GetNode()->GetDefaultAddress())
        .Item("start_time").Value(job->GetStartTime())
        .Item("account").Value(TmpAccountName)
        .Item("progress").Value(job->GetProgress())
        .DoIf(job->GetFinishTime().HasValue(), [=] (TFluentMap fluent) {
            fluent.Item("finish_time").Value(job->GetFinishTime().Get());
        })
        .DoIf(state == EJobState::Failed, [=] (TFluentMap fluent) {
            auto error = FromProto<TError>(job->Status()->result().error());
            fluent.Item("error").Value(error);
        })
        .DoIf(static_cast<bool>(inputPaths), [=] (TFluentMap fluent) {
            fluent.Item("input_paths").Value(*inputPaths);
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

static void BuildReadLimit(const TInputSlicePtr& slice, const TReadLimit& limit, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(limit.HasRowIndex(), [&] (TFluentMap fluent) {
                fluent
                    .Item("row_index").Value(limit.GetRowIndex() + slice->GetInputChunk()->GetTableRowIndex());
            })
            .DoIf(limit.HasKey(), [&] (TFluentMap fluent) {
                fluent
                    .Item("key").Value(limit.GetKey());
            })
        .EndMap();
}

TYsonString BuildInputPaths(
    const std::vector<TRichYPath>& inputPaths,
    const TChunkStripeListPtr& inputStripeList)
{
    std::vector<std::vector<TInputSlicePtr>> slicesByTable(inputPaths.size());
    for (const auto& stripe : inputStripeList->Stripes) {
        for (const auto& slice : stripe->ChunkSlices) {
            if (slice->GetInputChunk()->GetTableIndex() >= 0) {
                slicesByTable[slice->GetInputChunk()->GetTableIndex()].push_back(slice);
            }
        }
    }

    std::vector<std::vector<std::pair<TInputSlicePtr, TInputSlicePtr>>> rangesByTable(inputPaths.size());
    for (int tableIndex = 0; tableIndex < static_cast<int>(slicesByTable.size()); ++tableIndex) {
        auto& tableSlices = slicesByTable[tableIndex];

        std::sort(tableSlices.begin(), tableSlices.end(), &CompareSlicesByLowerLimit);

        int firstSlice = 0;
        while (firstSlice < static_cast<int>(tableSlices.size())) {
            int lastSlice = firstSlice + 1;
            while (lastSlice < static_cast<int>(tableSlices.size())) {
                if (!CanMergeSlices(tableSlices[lastSlice - 1], tableSlices[lastSlice])) {
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
                    fluent
                        .Item()
                        .BeginAttributes()
                            .Item("ranges")
                            .DoListFor(tableRanges, [&] (TFluentList fluent, const std::pair<TInputSlicePtr, TInputSlicePtr>& range) {
                                fluent
                                    .Item()
                                    .BeginMap()
                                        .Item("lower_limit")
                                        .Do(BIND(&BuildReadLimit, range.first, range.first->LowerLimit()))
                                        .Item("upper_limit")
                                        .Do(BIND(&BuildReadLimit, range.second, range.second->UpperLimit()))
                                    .EndMap();
                            })
                        .EndAttributes()
                        .Value(inputPaths[tableRanges[0].first->GetInputChunk()->GetTableIndex()].GetPath());
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
    return error.Attributes().Get<EAbortReason>("abort_reason", EAbortReason::Scheduler);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

