#pragma once

#include "fwd.h"
#include "bigrt_memory_result.h"

#include <yt/cpp/roren/bigrt/graph/parser.h>
#include <yt/cpp/roren/interface/roren.h>

#include <util/generic/ptr.h>

#include <vector>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TBigRtExecutor
{
public:
    ~TBigRtExecutor();

    void Start(IExecutionContextPtr context);

    template <typename T>
    void DoBatch(std::span<const T> range)
    {
        RawParDo_->Do(range.data(), range.size());
    }

    template <typename T>
    void Do(const T& t)
    {
        DoBatch(std::span<const T>(&t, 1));
    }

    TBigRtMemoryResultPtr Finish();

private:
    void TypeCheck(const std::type_info& typeInfo);

    TString GetInputTypeName() const;

private:
    using TRawOutputFactory = std::function<NPrivate::IRawOutputPtr(NPrivate::TBigRtMemoryStorage&)>;

private:
    TBigRtExecutor(
        NPrivate::IRawParDoPtr rawParDo,
        const std::vector<TRawOutputFactory>& factories,
        TBigRtExecutorPoolPtr executorPool);

    TBigRtExecutor(const TBigRtExecutor&) = delete;

    void Release();

private:
    NPrivate::TBigRtMemoryStorage ResharderMemoryStorage_;
    std::vector<NPrivate::IRawOutputPtr> RawOutputs_;
    IExecutionContextPtr ExecutionContext_;
    NPrivate::IRawParDoPtr RawParDo_;
    TBigRtExecutorPoolPtr ExecutorPool_;

    mutable std::optional<std::type_index> CachedTypeIndex_;

private:
    friend class TBigRtExecutorPool;
};

////////////////////////////////////////////////////////////////////////////////

class TBigRtExecutorPool
    : public TThrRefBase
{
public:
    explicit TBigRtExecutorPool(const NPrivate::TBigRtResharderDescription& description);

    TBigRtExecutor AcquireExecutor();

private:
    void ReleaseParDo(NPrivate::IRawParDoPtr rawParDo);

private:
    using TRawOutputFactory = TBigRtExecutor::TRawOutputFactory;

    std::unique_ptr<TClonablePool<NPrivate::IRawParDo>> ClonablePool_;
    std::vector<TRawOutputFactory> RawOutputFactories_;

private:
    friend TBigRtExecutor;
};

TBigRtExecutorPoolPtr MakeExecutorPool(const TPipeline& pipeline);
TBigRtExecutorPoolPtr MakeExecutorPool(const NPrivate::TBigRtResharderDescription& description);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
