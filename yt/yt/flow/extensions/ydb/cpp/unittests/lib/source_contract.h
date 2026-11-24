#pragma once

#include "fakes.h"

#include <yt/yt/flow/extensions/ydb/cpp/source_engine.h>

#include <yt/yt/flow/library/cpp/common/source.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

namespace NYT::NFlow::NYdbTesting {

////////////////////////////////////////////////////////////////////////////////

inline constexpr auto SourceRunDeadline = TDuration::Seconds(60);
inline constexpr TStringBuf MalformedLenvalPayload = "\xff\xff\xff\xff truncated";

extern const TStreamId SourceRunStreamId;

std::string PackLenvalRecord(const std::vector<std::string>& frames);

TDynamicSourceContextPtr MakeDynamicSourceContext(
    const std::vector<std::pair<std::string, NYTree::INodePtr>>& parameters = {},
    NYTree::IMapNodePtr partitionSpec = nullptr);

i64 GetRecordOffset(const ISource::TMessageBatch& batch);

////////////////////////////////////////////////////////////////////////////////

struct TSourceFamily
{
    std::string SourceClassName;
    NYTree::IMapNodePtr Parameters;
    TKey SourceKey;
    IResourcePtr Driver;
    std::function<std::string(const NYson::TYsonString&)> ReadUnparsedData;
};

////////////////////////////////////////////////////////////////////////////////

class TSourceRun
{
public:
    TSourceRun(
        const TSourceFamily& family,
        const TDynamicSourceContextPtr& dynamicContext,
        TStateManagerMockPtr stateManager,
        bool stableReadGroups = false);
    ~TSourceRun();

    template <class TFunction>
    auto RunInQueue(TFunction&& function)
    {
        auto result = NConcurrency::WaitFor(BIND(std::forward<TFunction>(function)).AsyncVia(Invoker_).Run());
        if constexpr (requires { result.ValueOrThrow(); }) {
            return result.ValueOrThrow();
        } else {
            result.ThrowOnError();
        }
    }

    std::vector<ISource::TMessageBatch> ReadBatches(int batchCount);
    void Persist(const std::vector<ISource::TMessageBatch>& batches);
    void Terminate();

    const ISourcePtr& GetSource() const;
    const THoldingInvokerPtr& GetInvoker() const;

private:
    const NConcurrency::TActionQueuePtr Queue_ = New<NConcurrency::TActionQueue>("YdbSourceRun");
    const NConcurrency::IThreadPoolPtr DecompressionPool_ = NConcurrency::CreateThreadPool(2, "YdbSourceRunPool");
    const THoldingInvokerPtr Invoker_ = New<THoldingInvoker>(Queue_->GetInvoker());
    const TStateManagerMockPtr StateManager_;

    ISourcePtr Source_;
    bool Terminated_ = false;
};

////////////////////////////////////////////////////////////////////////////////

void CheckRepositionedPartitionUpdate(
    const TSourceFamily& family,
    const std::function<void(const ISourcePtr&, const TYdbPartitionInfoUpdate&)>& applyUpdate);

//! |readEffectiveOptions| returns the current partition limit and malformed message policy of the source.
void CheckRejectedPartitionSpec(
    const TSourceFamily& family,
    const std::function<std::pair<int, std::string>(const ISourcePtr&)>& readEffectiveOptions);

void CheckKeptMalformedPayload(const TSourceFamily& family);

//! Expects the topic to hold a valid record, two malformed records and another valid record.
void CheckQueuedSourceReconfigurations(const TSourceFamily& family);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTesting
