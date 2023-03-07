#include "evaluator.h"
#include "private.h"
#include "config.h"
#include "evaluation_helpers.h"
#include "folding_profiler.h"
#include "helpers.h"
#include "query.h"

#include <yt/client/query_client/query_statistics.h>

#include <yt/client/table_client/unversioned_writer.h>

#include <yt/core/profiling/timing.h>

#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/finally.h>
#include <yt/core/misc/memory_usage_tracker.h>

#include <llvm/ADT/FoldingSet.h>

#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/Threading.h>

#include <utility>

namespace NYT::NQueryClient {
////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NProfiling;

using NNodeTrackerClient::TNodeMemoryTracker;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMemoryProviderMapByTag)
DECLARE_REFCOUNTED_CLASS(TTrackedMemoryChunkProvider)

////////////////////////////////////////////////////////////////////////////////

class TTrackedMemoryChunkProvider
    : public IMemoryChunkProvider
{
private:
    struct THolder
        : public TAllocationHolder
    {
        THolder(
            TMutableRef ref,
            TRefCountedTypeCookie cookie)
            : TAllocationHolder(ref, cookie)
        { }

        ~THolder()
        {
            if (!Owner) {
                return;
            }

            Owner->Allocated_ -= GetRef().Size();
            if (Owner->MemoryTracker_) {
                Owner->MemoryTracker_->Release(GetRef().Size());
            }
        }

        TIntrusivePtr<TTrackedMemoryChunkProvider> Owner;
    };

public:
    TTrackedMemoryChunkProvider(
        TString key,
        TMemoryProviderMapByTagPtr parent,
        size_t limit,
        IMemoryUsageTrackerPtr memoryTracker)
        : Key_(std::move(key))
        , Parent_(std::move(parent))
        , Limit_(limit)
        , MemoryTracker_(std::move(memoryTracker))
    { }

    virtual std::unique_ptr<TAllocationHolder> Allocate(size_t size, TRefCountedTypeCookie cookie) override
    {
        size_t allocated = Allocated_.load();
        do {
            if (allocated + size > Limit_) {
                THROW_ERROR_EXCEPTION("Not enough memory to serve allocation",
                    size,
                    allocated,
                    Limit_)
                    << TErrorAttribute("allocation_size", size)
                    << TErrorAttribute("allocated", allocated)
                    << TErrorAttribute("limit", Limit_);
            }
        } while (!Allocated_.compare_exchange_weak(allocated, allocated + size));

        std::unique_ptr<THolder> result(TAllocationHolder::Allocate<THolder>(size, cookie));
        auto allocatedSize = result->GetRef().Size();
        YT_VERIFY(allocatedSize != 0);

        auto delta = allocatedSize - size;
        allocated = Allocated_.fetch_add(delta) + delta;

        auto maxAllocated = MaxAllocated_.load();
        while (maxAllocated < allocated && !MaxAllocated_.compare_exchange_weak(maxAllocated, allocated));

        auto finally = Finally([&] {
            Allocated_ -= allocatedSize;
        });

        if (MemoryTracker_) {
            MemoryTracker_->TryAcquire(allocatedSize)
                .ThrowOnError();
        }

        finally.Release();
        result->Owner = this;

        return result;
    }

    size_t GetMaxAllocated() const
    {
        return MaxAllocated_;
    }

    ~TTrackedMemoryChunkProvider();

private:
    const TString Key_;
    const TMemoryProviderMapByTagPtr Parent_;
    const size_t Limit_;
    const IMemoryUsageTrackerPtr MemoryTracker_;

    std::atomic<size_t> Allocated_ = {0};
    std::atomic<size_t> MaxAllocated_ = {0};
};

DEFINE_REFCOUNTED_TYPE(TTrackedMemoryChunkProvider)

////////////////////////////////////////////////////////////////////////////////

class TMemoryProviderMapByTag
    : public TRefCounted
{
public:
    TTrackedMemoryChunkProviderPtr GetProvider(
        const TString& tag,
        size_t limit,
        IMemoryUsageTrackerPtr memoryTracker)
    {
        auto guard = Guard(SpinLock_);
        auto it = Map_.emplace(tag, nullptr).first;

        auto result = it->second.Lock();

        if (!result) {
            result = New<TTrackedMemoryChunkProvider>(tag, this, limit, std::move(memoryTracker));
            it->second = result;
        }

        return result;
    }

    friend class TTrackedMemoryChunkProvider;

private:
    TSpinLock SpinLock_;
    THashMap<TString, TWeakPtr<TTrackedMemoryChunkProvider>> Map_;
};

DEFINE_REFCOUNTED_TYPE(TMemoryProviderMapByTag);

TTrackedMemoryChunkProvider::~TTrackedMemoryChunkProvider()
{
    auto guard = Guard(Parent_->SpinLock_);
    Parent_->Map_.erase(Key_);
}

////////////////////////////////////////////////////////////////////////////////

class TCachedCGQuery
    : public TAsyncCacheValueBase<
        llvm::FoldingSetNodeID,
        TCachedCGQuery>
{
public:
    TCachedCGQuery(const llvm::FoldingSetNodeID& id, TCGQueryCallback&& function)
        : TAsyncCacheValueBase(id)
        , Function_(std::move(function))
    { }

    TCGQueryCallback GetQueryCallback()
    {
        return Function_;
    }

private:
    TCGQueryCallback Function_;
};

typedef TIntrusivePtr<TCachedCGQuery> TCachedCGQueryPtr;

class TEvaluator::TImpl
    : public TAsyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedCGQuery>
{
public:
    TImpl(
        const TExecutorConfigPtr& config,
        const NProfiling::TProfiler& profiler,
        IMemoryUsageTrackerPtr memoryTracker)
        : TAsyncSlruCacheBase(config->CGCache, profiler.AppendPath("/cg_cache"))
        , MemoryTracker_(std::move(memoryTracker))
    { }

    TQueryStatistics Run(
        const TConstBaseQueryPtr& query,
        const ISchemafulReaderPtr& reader,
        const IUnversionedRowsetWriterPtr& writer,
        const TJoinSubqueryProfiler& joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        const TQueryBaseOptions& options)
    {
        NTracing::TChildTraceContextGuard guard("QueryClient.Evaluate");
        NTracing::AddTag("fragment_id", ToString(query->Id));
        auto queryFingerprint = InferName(query, true);
        NTracing::AddTag("query_fingerprint", queryFingerprint);

        auto Logger = MakeQueryLogger(query);

        YT_LOG_DEBUG("Executing query (Fingerprint: %v, ReadSchema: %v, ResultSchema: %v)",
            queryFingerprint,
            query->GetReadSchema(),
            query->GetTableSchema());

        TQueryStatistics statistics;
        NProfiling::TWallTimer wallTime;
        NProfiling::TFiberWallTimer syncTime;

        auto finalLogger = Finally([&] {
            YT_LOG_DEBUG("Finalizing evaluation");
        });

        auto memoryChunkProvider = MemoryProvider_->GetProvider(
            ToString(options.ReadSessionId),
            options.MemoryLimitPerNode,
            MemoryTracker_);

        try {
            TCGVariables fragmentParams;
            auto cgQuery = Codegen(
                query,
                fragmentParams,
                joinProfiler,
                functionProfilers,
                aggregateProfilers,
                statistics,
                options.EnableCodeCache,
                options.UseMultijoin);

            auto finalizer = Finally([&] () {
                fragmentParams.Clear();
            });

            // NB: function contexts need to be destroyed before cgQuery since it hosts destructors.
            TExecutionContext executionContext;
            executionContext.Reader = reader;
            executionContext.Writer = writer;
            executionContext.Statistics = &statistics;
            executionContext.InputRowLimit = options.InputRowLimit;
            executionContext.OutputRowLimit = options.OutputRowLimit;
            executionContext.GroupRowLimit = options.OutputRowLimit;
            executionContext.JoinRowLimit = options.OutputRowLimit;
            executionContext.Offset = query->Offset;
            executionContext.Limit = query->Limit;
            executionContext.Ordered = query->IsOrdered();
            executionContext.IsMerge = dynamic_cast<const TFrontQuery*>(query.Get());
            executionContext.MemoryChunkProvider = memoryChunkProvider;

            YT_LOG_DEBUG("Evaluating query");

            CallCGQueryPtr(
                cgQuery,
                fragmentParams.GetLiteralValues(),
                fragmentParams.GetOpaqueData(),
                &executionContext);

        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Query evaluation failed");
            THROW_ERROR_EXCEPTION("Query evaluation failed") << ex;
        }

        statistics.SyncTime = syncTime.GetElapsedTime();
        statistics.AsyncTime = wallTime.GetElapsedTime() - statistics.SyncTime;
        statistics.ExecuteTime =
            statistics.SyncTime - statistics.ReadTime - statistics.WriteTime - statistics.CodegenTime;

        statistics.MemoryUsage = memoryChunkProvider->GetMaxAllocated();

        YT_LOG_DEBUG("Query statistics (%v)", statistics);

        // TODO(prime): place these into trace log
        //    TRACE_ANNOTATION("rows_read", statistics.RowsRead);
        //    TRACE_ANNOTATION("rows_written", statistics.RowsWritten);
        //    TRACE_ANNOTATION("sync_time", statistics.SyncTime);
        //    TRACE_ANNOTATION("async_time", statistics.AsyncTime);
        //    TRACE_ANNOTATION("execute_time", statistics.ExecuteTime);
        //    TRACE_ANNOTATION("read_time", statistics.ReadTime);
        //    TRACE_ANNOTATION("write_time", statistics.WriteTime);
        //    TRACE_ANNOTATION("codegen_time", statistics.CodegenTime);
        //    TRACE_ANNOTATION("incomplete_input", statistics.IncompleteInput);
        //    TRACE_ANNOTATION("incomplete_output", statistics.IncompleteOutput);

        return statistics;
    }

private:
    const IMemoryUsageTrackerPtr MemoryTracker_;
    const TMemoryProviderMapByTagPtr MemoryProvider_ = New<TMemoryProviderMapByTag>();


    TCGQueryCallback Codegen(
        TConstBaseQueryPtr query,
        TCGVariables& variables,
        const TJoinSubqueryProfiler& joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        TQueryStatistics& statistics,
        bool enableCodeCache,
        bool useMultijoin)
    {
        llvm::FoldingSetNodeID id;

        auto makeCodegenQuery = Profile(
            query,
            &id,
            &variables,
            joinProfiler,
            useMultijoin,
            functionProfilers,
            aggregateProfilers);

        auto Logger = MakeQueryLogger(query);

        auto compileWithLogging = [&] () {
            NTracing::TChildTraceContextGuard traceContextGuard("QueryClient.Compile");

            YT_LOG_DEBUG("Started compiling fragment");
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics.CodegenTime);
            auto cgQuery = New<TCachedCGQuery>(id, makeCodegenQuery());
            YT_LOG_DEBUG("Finished compiling fragment");
            return cgQuery;
        };

        TCachedCGQueryPtr cgQuery;
        if (enableCodeCache) {
            auto cookie = BeginInsert(id);
            if (cookie.IsActive()) {
                YT_LOG_DEBUG("Codegen cache miss: generating query evaluator");

                try {
                    cookie.EndInsert(compileWithLogging());
                } catch (const std::exception& ex) {
                    cookie.Cancel(TError(ex).Wrap("Failed to compile a query fragment"));
                }
            }

            cgQuery = WaitFor(cookie.GetValue())
                .ValueOrThrow();
        } else {
            YT_LOG_DEBUG("Codegen cache disabled");

            cgQuery = compileWithLogging();
        }

        return cgQuery->GetQueryCallback();
    }

    static void CallCGQuery(
        const TCGQueryCallback& cgQuery,
        TValue* literals,
        void* const* opaqueValues,
        TExecutionContext* executionContext)
    {
        cgQuery(literals, opaqueValues, executionContext);
    }

    void(*volatile CallCGQueryPtr)(
        const TCGQueryCallback& cgQuery,
        TValue* literals,
        void* const* opaqueValues,
        TExecutionContext* executionContext) = CallCGQuery;
};

////////////////////////////////////////////////////////////////////////////////

TEvaluator::TEvaluator(
    TExecutorConfigPtr config,
    const NProfiling::TProfiler& profiler,
    IMemoryUsageTrackerPtr memoryTracker)
    : Impl_(New<TImpl>(
        std::move(config),
        profiler,
        std::move(memoryTracker)))
{ }

TEvaluator::~TEvaluator()
{ }

TQueryStatistics TEvaluator::Run(
    TConstBaseQueryPtr query,
    ISchemafulReaderPtr reader,
    IUnversionedRowsetWriterPtr writer,
    TJoinSubqueryProfiler joinProfiler,
    TConstFunctionProfilerMapPtr functionProfilers,
    TConstAggregateProfilerMapPtr aggregateProfilers,
    const TQueryBaseOptions& options)
{
    return Impl_->Run(
        std::move(query),
        std::move(reader),
        std::move(writer),
        std::move(joinProfiler),
        std::move(functionProfilers),
        std::move(aggregateProfilers),
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
