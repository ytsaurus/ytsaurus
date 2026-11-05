#include "timer_store_conversion.h"

#include "paged_timer_store.h"
#include "timer_store.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/flow/library/cpp/tables/timers.h>
#include <yt/yt/flow/library/cpp/tables/transaction_manager.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

NTables::ITimers::TFilter MakePartitionFilter(const TTimerStoreContext& context)
{
    return NTables::ITimers::TFilter{
        .ComputationId = context.Partition->ComputationId,
        .LowerKey = context.Partition->LowerKey,
        .ExactKey = context.Partition->SourceKey,
        .UpperKey = context.Partition->UpperKey,
    };
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ImportTimersIntoPages(const TTimerStoreContextPtr& context, const IPagedTimerStorePtr& pagedStore)
{
    const auto& Logger = context->Logger;
    i64 imported = 0;
    std::optional<NTables::ITimers::TTableKey> offset;
    while (true) {
        // Each load continues after the previous batch: starting over would scan the deletion
        // marks of every batch imported so far.
        auto result = WaitFor(context->TimersTable->Load(MakePartitionFilter(*context), TimerConversionBatchSize, offset))
            .ValueOrThrow();
        if (result.Timers.empty()) {
            break;
        }
        std::vector<TTimer> timers;
        std::vector<NTables::ITimers::TTableKey> keys;
        for (auto& [tableKey, timer] : result.Timers) {
            if (!context->TimerSpecs.contains(timer.StreamId)) {
                continue;
            }
            timer.KeySchema = context->KeySchema;
            timers.push_back(std::move(timer));
            keys.push_back(std::move(tableKey));
        }
        imported += std::ssize(timers);
        if (!timers.empty()) {
            pagedStore->Register(std::move(timers));
            // The maintenance splits the pages the batch has overfilled, within the same commit.
            pagedStore->PreloadSync();
            pagedStore->SyncAndCommit([&] (const NApi::IDynamicTableTransactionPtr& transaction) {
                context->TimersTable->Erase(transaction, keys);
            });
        }
        if (!result.ContinuationOffsetExclusive) {
            break;
        }
        offset = std::move(result.ContinuationOffsetExclusive);
    }
    if (imported) {
        YT_TLOG_INFO("Timers imported into pages")
            .With("Count", imported);
    }
}

void ExportPagesIntoTimers(const TTimerStoreContextPtr& context, const IPagedTimerStorePtr& pagedStore)
{
    const auto& Logger = context->Logger;
    THashSet<TStreamId> streams;
    for (const auto& [streamId, spec] : context->TimerSpecs) {
        streams.insert(streamId);
    }

    i64 exported = 0;
    int idleRounds = 0;
    while (true) {
        pagedStore->TriggerAll();
        auto batch = pagedStore->GetNextBatch(streams, TimerConversionBatchSize, std::numeric_limits<i64>::max());
        if (batch.empty()) {
            if (pagedStore->IsEmpty()) {
                break;
            }
            // Pages beyond the resident window become resident a few per preload; shared
            // pages holding nothing for the partition may run for long.
            if (pagedStore->PreloadSync()) {
                idleRounds = 0;
            } else {
                THROW_ERROR_EXCEPTION_IF(++idleRounds > 100, "Timer pages cannot be exhausted during the export");
            }
            continue;
        }
        idleRounds = 0;
        std::vector<TTimer> timers;
        for (const auto& timer : batch) {
            timers.push_back(TTimer(*timer));
        }
        exported += std::ssize(timers);
        pagedStore->PreloadSync();
        pagedStore->Unregister(batch);
        pagedStore->SyncAndCommit([&] (const NApi::IDynamicTableTransactionPtr& transaction) {
            context->TimersTable->Write(transaction, context->Partition->ComputationId, timers);
        });
    }
    pagedStore->DropEmptyPages();
    pagedStore->SyncAndCommit([] (const NApi::IDynamicTableTransactionPtr& /*transaction*/) {
    });
    YT_TLOG_INFO("Timers exported from pages")
        .With("Count", exported);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
