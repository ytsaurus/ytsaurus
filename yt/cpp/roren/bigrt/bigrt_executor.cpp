#include "bigrt_executor.h"

#include "bigrt.h"
#include "clonable_pool.h"
#include "profiling.h"

#include <yt/cpp/roren/bigrt/graph/parser.h>

#include <util/generic/ptr.h>

#include <memory>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TBigRtExecutor::TBigRtExecutor(
    NPrivate::IRawParDoPtr rawParDo,
    const std::vector<TRawOutputFactory>& factories,
    TBigRtExecutorPoolPtr executorPool)
    : RawParDo_(std::move(rawParDo))
    , ExecutorPool_(executorPool)
{
    for (const auto& f : factories) {
        RawOutputs_.push_back(f(ResharderMemoryStorage_));
    }
}

TBigRtExecutor::~TBigRtExecutor()
{
    Release();
}

void TBigRtExecutor::Start(IExecutionContextPtr context)
{
    ExecutionContext_ = std::move(context);
    RawParDo_->Start(ExecutionContext_, RawOutputs_);
}

TBigRtMemoryResultPtr TBigRtExecutor::Finish()
{
    RawParDo_->Finish();
    for (const auto& output : RawOutputs_) {
        output->Close();
    }
    Release();
    NPrivate::FlushProfiler(ExecutionContext_->GetProfiler());
    return ResharderMemoryStorage_.ToResult();
}

void TBigRtExecutor::TypeCheck(const std::type_info& typeInfo)
{
    auto throwBadType = [&] {
        ythrow yexception() << "Type error, expected: " << GetInputTypeName() << " actual: " << typeInfo.name();
    };

    if (CachedTypeIndex_) {
        if (typeInfo != *CachedTypeIndex_) {
            throwBadType();
        }
    } else {
        if (GetInputTypeName() != typeInfo.name()) {
            throwBadType();
        }

        CachedTypeIndex_.emplace(typeInfo);
    }
}

TString TBigRtExecutor::GetInputTypeName() const
{
    auto inputTags = RawParDo_->GetInputTags();
    Y_VERIFY(inputTags.size() == 1);
    const auto& rowVtable = inputTags[0].GetRowVtable();
    return rowVtable.TypeName;
}

void TBigRtExecutor::Release()
{
    if (RawParDo_) {
        ExecutorPool_->ReleaseParDo(RawParDo_);
        RawParDo_ = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

NPrivate::TBigRtResharderDescription ParseTheOnlyResharderDescription(const TPipeline& pipeline)
{
    auto parsed = NRoren::NPrivate::ParseBigRtResharder(pipeline);
    // TODO: we need better parsing
    Y_VERIFY(parsed.size() == 1);
    return parsed[0];
}

TBigRtExecutorPool::TBigRtExecutorPool(const NPrivate::TBigRtResharderDescription& description)
{
    ClonablePool_ = std::make_unique<TClonablePool<NPrivate::IRawParDo>>(description.Resharder);

    for (const auto& writer : description.Writers) {
        const auto& factory = NPrivate::GetRequiredAttribute(*writer, NPrivate::MemoryOutputFactoryTag);
        RawOutputFactories_.push_back(factory);
    }
}

TBigRtExecutor TBigRtExecutorPool::AcquireExecutor()
{
    auto rawParDo = ClonablePool_->Acquire();
    return TBigRtExecutor{
        rawParDo,
        RawOutputFactories_,
        this
    };
}

void TBigRtExecutorPool::ReleaseParDo(NPrivate::IRawParDoPtr rawParDo)
{
    ClonablePool_->Release(rawParDo);
}

////////////////////////////////////////////////////////////////////////////////

TBigRtExecutorPoolPtr MakeExecutorPool(const TPipeline& pipeline)
{
    auto description = ParseTheOnlyResharderDescription(pipeline);
    Y_VERIFY(description.WriterRegistratorList.empty(), "Resharder has nontrivial writers and must be created with CompositeWriterFactory");
    return ::MakeIntrusive<TBigRtExecutorPool>(description);
}

TBigRtExecutorPoolPtr MakeExecutorPool(const NPrivate::TBigRtResharderDescription& description)
{
    return ::MakeIntrusive<TBigRtExecutorPool>(description);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
