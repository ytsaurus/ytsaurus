#include "mkql_ytflow_chunked_forward_list.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

using namespace NYql;
using namespace NYql::NUdf;

namespace {

struct TStreamState
{
    TUnboxedValue Stream;

    bool PendingYield = false;
    bool PendingFinish = false;
};

class TChunkListValue
    : public TComputationValue<TChunkListValue>
{
public:
    TChunkListValue(
        TMemoryUsageInfo* memInfo,
        TStreamState* streamState
    )
        : TComputationValue(memInfo)
        , StreamState(streamState)
    {
        MKQL_ENSURE(StreamState, "Null stream state");
    }

private:
    TUnboxedValue GetListIterator() const override {
        MKQL_ENSURE(!HasListIterator, "Only one pass over input is supported");
        HasListIterator = true;

        return TUnboxedValuePod(const_cast<TChunkListValue*>(this));
    }

    bool Next(TUnboxedValue& value) override {
        auto status = StreamState->Stream.Fetch(value);
        bool isValid = true;

        switch (status) {
        case EFetchStatus::Ok:
            break;

        case EFetchStatus::Yield:
            StreamState->PendingYield = true;
            isValid = false;
            break;

        case EFetchStatus::Finish:
            StreamState->PendingFinish = true;
            isValid = false;
            break;
        }

        return isValid;
    }

private:
    TStreamState* StreamState = nullptr;

    mutable bool HasListIterator = false;
};

class TYtflowChunkedForwardListWrapper
    : public TMutableComputationNode<TYtflowChunkedForwardListWrapper>
{
public:
    using TBase = TMutableComputationNode<TYtflowChunkedForwardListWrapper>;
    using TSelf = TYtflowChunkedForwardListWrapper;

    class TStreamValue
        : public TComputationValue<TStreamValue>
    {
    public:
        TStreamValue(
            TMemoryUsageInfo* memInfo,
            TUnboxedValue stream
        )
            : TComputationValue(memInfo)
            , StreamState(TStreamState{
                .Stream = std::move(stream)
            })
        {
        }

        EFetchStatus Fetch(TUnboxedValue& result) override {
            if (StreamState.PendingYield) {
                StreamState.PendingYield = false;
                return EFetchStatus::Yield;
            }

            if (StreamState.PendingFinish) {
                StreamState.PendingFinish = false;
                return EFetchStatus::Finish;
            }

            result = NUdf::TUnboxedValuePod(
                new TChunkListValue(GetMemInfo(), &StreamState));

            return EFetchStatus::Ok;
        }

    private:
        TStreamState StreamState;
    };

    TYtflowChunkedForwardListWrapper(
        TComputationMutables& mutables,
        IComputationNode* stream
    )
        : TBase(mutables)
        , Stream(stream)
    {
    }

    void RegisterDependencies() const override {
        DependsOn(Stream);
    }

    TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(Stream->GetValue(ctx));
    }

private:
    IComputationNode* Stream;
};

} // anonymous namespace

IComputationNode* WrapYtflowChunkedForwardList(
    TCallable& callable,
    const TComputationNodeFactoryContext& ctx
) {
    MKQL_ENSURE(
        callable.GetInputsCount() == 1,
        "Unexpected inputs count: " << callable.GetInputsCount());

    MKQL_ENSURE(
        callable.GetInput(0).GetStaticType()->IsStream(),
        "Unexpected input type: " << callable.GetInput(0).GetStaticType()->GetKindAsStr());

    auto* stream = LocateNode(ctx.NodeLocator, callable, 0);

    return new TYtflowChunkedForwardListWrapper(ctx.Mutables, stream);
}

} // namespace NKikimr::NMiniKQL
