#pragma once

#include "fwd.h"

#include "bigrt_memory_result.h"
#include "config_builder.h"
#include "stateful_impl/stateful_par_do.h"
#include "program.h"
#include "transforms.h"
#include "writers.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/interface/transforms.h>
#include <yt/cpp/roren/bigrt/graph/parser.h>

#include <yt/yt/core/actions/public.h>

#include <bigrt/lib/processing/state_manager/base/manager.h>
#include <bigrt/lib/processing/state_manager/base/request.h>

#include <util/generic/ptr.h>
#include <util/ysaveload.h>

#include <type_traits>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Usual read transform for BigRt pipelines. Provides message batches.
///
/// @param supplierConfig configuration describing where pipeline will read data from and how it will do it
///   for detailed description see comments in .proto file describing this config.
TReadTransform<NBigRT::TMessageBatch> ReadMessageBatch(const TString& inputTag = "input");

///
/// @brief Creates pipeline that stores bigrt graph but cannot be executed.
///
/// Returned pipeline must be runned manualy using @ref NYT::TBigRtExecutorPool
TPipeline MakeBigRtDummyPipeline();

///
/// Write to memory. Used with @ref NRoren::MakeBigRtDummyPipeline.
template <typename T>
TBigRtMemoryWriteTransform<T> ResharderMemoryWrite(TTypeTag<T> tag);


template <typename TKey, typename TState>
TPState<TKey, TState> MakeBigRtPState(const TPipeline& pipeline, TCreateStateManagerFunction<TKey, TState> createStateManagerFunc, const TBigRtStateConfig& config = {});

////////////////////////////////////////////////////////////////////////////////

// || |\    /| |==\\ ||     |=== |\    /| ||=== |\ || ====   /\   ====  ||  //\\  |\ ||
// || ||\  /|| |==// ||    ||=== ||\  /|| ||=== ||\||  ||   /__\   ||   || ||  || ||\||
// || || \/ || ||    ||===  |=== || \/ || ||=== || \|  ||  //  \\  ||   ||  \\//  || \|

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {
    extern TTypeTag<std::function<IRawOutputPtr(TBigRtMemoryStorage&)>> MemoryOutputFactoryTag;

    extern TTypeTag<TCreateBaseStateManagerFunction> CreateBaseStateManagerFunctionTag;
    extern TTypeTag<TBigRtStateManagerVtable> BigRtStateManagerVtableTag;
    extern TTypeTag<TBigRtStateConfig> BigRtStateConfigTag;

    template <typename TKey, typename TState>
    class TBigRtStateStore
        : public IRawStateStore
    {
    private:
        using TRequestPtr = NBigRT::TGenericStateRequestPtr<TKey, TState>;

    public:
        void* GetState(const void *rawKey) override
        {
            const auto* key = static_cast<const TKey*>(rawKey);
            auto it = RequestMap_.find(*key);
            if (it == RequestMap_.end()) {
                return nullptr;
            } else {
                return &it->second->GetState();
            }
        }

        void Put(TRequestPtr request)
        {
            const auto& key = request->GetStateId();
            const auto* requestPtr = request.Get();
            auto [it, inserted] = RequestMap_.emplace(key, std::move(request));
            if (!inserted) {
                YT_VERIFY(requestPtr == it->second.Get());
            }
        }

    private:
        THashMap<TKey, TRequestPtr> RequestMap_;
    };
}

template <typename T>
class TBigRtMemoryWriteTransform
{
public:
    TBigRtMemoryWriteTransform(TTypeTag<T> typeTag)
        : TypeTag_(std::move(typeTag))
    { }

    void ApplyTo(const TPCollection<T>& pCollection) const
    {
        auto writeTransform = DummyWrite<T>();

        std::function<NPrivate::IRawOutputPtr(NPrivate::TBigRtMemoryStorage&)> factory;
        factory = [tag=TypeTag_] (NPrivate::TBigRtMemoryStorage& out) {
            return out.AddOutput(tag);
        };

        NPrivate::SetAttribute(writeTransform, NPrivate::MemoryOutputFactoryTag, factory);

        pCollection | writeTransform;
    }

private:
    TTypeTag<T> TypeTag_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TBigRtMemoryWriteTransform<T> ResharderMemoryWrite(TTypeTag<T> tag)
{
    return {tag};
}

template <typename TKey, typename TState>
TPState<TKey, TState> MakeBigRtPState(
    const TPipeline& bigRtPipeline,
    TCreateStateManagerFunction<TKey, TState> createStateManager,
    const TBigRtStateConfig& config)
{
    auto rawPipeline = NPrivate::GetRawPipeline(bigRtPipeline);
    auto pState = NPrivate::MakePState<TKey, TState>(rawPipeline);

    auto rawPState = NPrivate::GetRawPStateNode(pState);

    NPrivate::TCreateBaseStateManagerFunction createBaseStateManager = [createStateManager] (
        ui64 shard,
        const NSFStats::TSolomonContext& solomonContext
    ) -> NBigRT::TBaseStateManagerPtr {
        auto stateManager = createStateManager(shard, solomonContext);
        return static_cast<NBigRT::TBaseStateManagerPtr>(stateManager);
    };
    NPrivate::SetAttribute(*rawPState, NPrivate::CreateBaseStateManagerFunctionTag, createBaseStateManager);

    NPrivate::TBigRtStateManagerVtable stateVtable;
    stateVtable.KeyToRequest = [] (NBigRT::TBaseStateManager* baseStateManager, const void* rawKey) -> NBigRT::TBaseStateRequestPtr {
        auto* stateManager = static_cast<NBigRT::TGenericStateManager<TKey, TState>*>(baseStateManager);
        const auto* key = static_cast<const TKey*>(rawKey);
        auto request = stateManager->RequestState(*key);
        return std::move(request);
    };

    stateVtable.CreateStateStore = [] () -> NPrivate::IRawStateStorePtr {
        return MakeIntrusive<NPrivate::TBigRtStateStore<TKey, TState>>();
    };

    stateVtable.StateStorePut = [] (NPrivate::IRawStateStore* rawStateStore, NBigRT::TBaseStateRequest* baseRequest) {
        auto* stateStore = CheckedCast<NPrivate::TBigRtStateStore<TKey, TState>*>(rawStateStore);
        auto* request = CheckedCast<NBigRT::TGenericStateRequest<TKey, TState>*>(baseRequest);
        stateStore->Put(request);
    };

    NPrivate::SetAttribute(*rawPState, NPrivate::BigRtStateManagerVtableTag, stateVtable);
    NPrivate::SetAttribute(*rawPState, NPrivate::BigRtStateConfigTag, config);

    return pState;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
