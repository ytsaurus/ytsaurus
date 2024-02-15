#pragma once

#include "execution_context.h" // IWYU pragma: keep (clangd bug, required for SetExecutionContext)
#include "fwd.h"
#include "input.h"
#include "output.h"
#include "row.h"
#include "timers.h"
#include "private/fwd.h"
#include "private/concepts.h" // IWYU pragma: keep  (clangd bug, required for concepts)
#include "private/raw_state_store.h"
#include "timers.h"

#include <util/generic/function.h>

#include <span>

class IInputStream;
class IOutputStream;

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class IFnBase
    : public TThrRefBase
{
public:
    void SetExecutionContext(IExecutionContextPtr context)
    {
        Context_ = std::move(context);
    }

    const IExecutionContextPtr& GetExecutionContext() const
    {
        Y_ABORT_UNLESS(Context_);
        return Context_;
    }

    virtual void Save(IOutputStream*) const
    { }

    virtual void Load(IInputStream*)
    { }

private:
    IExecutionContextPtr Context_;
};

////////////////////////////////////////////////////////////////////////////////

//
// Do function
//

class TFnAttributes
{
public:
    TFnAttributes& SetIsPure(bool isPure = true);

    /// @brief Name of a function. Used in debug and info purposes.
    TFnAttributes& SetName(std::optional<TString> name) &;
    TFnAttributes SetName(std::optional<TString> name) &&;
    const std::optional<TString>& GetName() const;

    ///
    /// @brief Resource files are files that are required to run user function.
    ///
    /// Executors that run pipelines on remote machines upload these files
    /// (they will be available in current working directory of the process).
    ///
    /// Executors that run pipelines on current machine ignore these files.
    ///
    /// Status by executors:
    ///   - Local executor :: ignores.
    ///   - YT executor :: uses.
    ///   - BigRT executor :: ignores.
    TFnAttributes& AddResourceFile(const TString& resourceFile)&;
    TFnAttributes AddResourceFile(const TString& resourceFile)&&;

private:
    bool IsPure_ = false;
    std::vector<TString> ResourceFileList_;
    std::optional<TString> Name_;

    friend NPrivate::TFnAttributesOps;

public:
    Y_SAVELOAD_DEFINE(Name_, ResourceFileList_);
};

template <CRow TInput_, typename TOutput_>
class IDoFn
    : public IFnBase
{
public:
    using TInputRow = TInput_;
    using TOutputRow = TOutput_;

public:
    std::vector<TDynamicTypeTag> GetOutputTags() const
    {
        if constexpr (std::is_same_v<TOutputRow, void>) {
            return {};
        } else {
            return {TTypeTag<TOutputRow>("do-fn-output")};
        }
    }

    virtual void Start(TOutput<TOutputRow>&)
    { }

    virtual void Do(const TInputRow& input, TOutput<TOutputRow>& output) = 0;

    virtual void Finish(TOutput<TOutputRow>&)
    { }

    virtual TFnAttributes GetDefaultAttributes() const
    {
        return {};
    }
};

template <CRow TInput_>
class IDoFn<TInput_, TMultiRow>
    : public IFnBase
{
public:
    using TInputRow = TInput_;
    using TOutputRow = TMultiRow;

public:
    virtual std::vector<TDynamicTypeTag> GetOutputTags() const = 0;

    virtual void Start(TOutput<TOutputRow>&)
    { }
    virtual void Do(const TInputRow& input, TOutput<TOutputRow>& output) = 0;
    virtual void Finish(TOutput<TOutputRow>&)
    { }

    virtual TFnAttributes GetDefaultAttributes() const
    {
        return {};
    }
};

template <typename F>
concept CParDoMultiOutputFunction = requires (F f, TMultiOutput& multiOutput)
{
    f({}, multiOutput);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TKey_, typename TState_>
class TStateStore
    : private NPrivate::IRawStateStore
{
public:
    using TKey = TKey_;
    using TState = TState_;

public:
    TStateStore() = delete;

    TState& GetState(const TKey& key)
    {
        static_assert(sizeof(*this) == sizeof(NPrivate::IRawStateStore));
        return *static_cast<TState*>(GetStateRaw(&key));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TInputRow_, class TOutputRow_, class TState_>
class IStatefulDoFn
    : public IFnBase
{
public:
    static_assert(NTraits::TIsTKV<TInputRow_>::value);
    using TInputRow = TInputRow_;
    using TKey = typename TInputRow::TKey;
    using TValue = typename TInputRow::TValue;
    using TOutputRow = TOutputRow_;
    using TState = TState_;

public:
    std::vector<TDynamicTypeTag> GetOutputTags() const
    {
        if constexpr (std::is_same_v<TOutputRow, void>) {
            return {};
        } else {
            return {TTypeTag<TOutputRow>("do-fn-output")};
        }
    }

    virtual void Start(TOutput<TOutputRow>& /*output*/)
    { }

    virtual void Do(const TInputRow& input, TOutput<TOutputRow>& output, TState& state) = 0;

    virtual void Finish(TOutput<TOutputRow>& /*output*/, TStateStore<typename TInputRow::TKey, TState>& /*stateMap*/)
    { }

    virtual TFnAttributes GetDefaultAttributes() const
    {
        return {};
    }
};

template <typename TInputRow_, typename TState_>
class IStatefulDoFn<TInputRow_, TMultiRow, TState_>
    : public IFnBase
{
public:
    static_assert(NTraits::TIsTKV<TInputRow_>::value);
    using TInputRow = TInputRow_;
    using TKey = typename TInputRow::TKey;
    using TValue = typename TInputRow::TValue;
    using TOutputRow = TMultiRow;
    using TState = TState_;

public:
    virtual std::vector<TDynamicTypeTag> GetOutputTags() const = 0;

    virtual void Start(TOutput<TOutputRow>& /*output*/)
    { }

    virtual void Do(const TInputRow& input, TOutput<TOutputRow>& output, TState& state) = 0;

    virtual void Finish(TOutput<TOutputRow>& /*output*/, TStateStore<TKey, TState>& /*stateMap*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TInputRow_, typename TAccumRow_, typename TOutputRow_>
class ICombineFn
    : public IFnBase
{
public:
    using TInputRow = TInputRow_;
    using TAccumRow = TAccumRow_;

    using TOutputRow = TOutputRow_;
public:
    virtual TAccumRow CreateAccumulator() = 0;
    virtual void AddInput(TAccumRow* accum, const TInputRow& input) = 0;
    virtual TAccumRow MergeAccumulators(TInput<TAccumRow>& accums) = 0;
    virtual TOutputRow ExtractOutput(const TAccumRow& accum) = 0;
};

template <typename TFn>
concept CCombineFnPtr = NPrivate::CIntrusivePtr<TFn> && requires (TFn t) {
    typename TFn::TValueType::TInputRow;
    typename TFn::TValueType::TAccumRow;
    typename TFn::TValueType::TOutputRow;
    std::is_base_of_v<
        ICombineFn<
            typename TFn::TValueType::TInputRow,
            typename TFn::TValueType::TAccumRow,
            typename TFn::TValueType::TOutputRow
        >,
        typename TFn::TValueType
    >;
};

////////////////////////////////////////////////////////////////////////////////

template <class TInputRow_, class TOutputRow_, class TState_>
class IStatefulTimerDoFn
    : public IStatefulDoFn<TInputRow_, TOutputRow_, TState_>
{
    public:
        using TInputRow = typename IStatefulDoFn<TInputRow_, TOutputRow_, TState_>::TInputRow;
        using TKey = typename IStatefulDoFn<TInputRow_, TOutputRow_, TState_>::TKey;
        using TValue = typename IStatefulDoFn<TInputRow_, TOutputRow_, TState_>::TValue;
        using TOutputRow = typename IStatefulDoFn<TInputRow_, TOutputRow_, TState_>::TOutputRow;
        using TState = typename IStatefulDoFn<TInputRow_, TOutputRow_, TState_>::TState;
        using IFnBase::GetExecutionContext;

        explicit IStatefulTimerDoFn(TString fnId)
            : FnId_(std::move(fnId))
        {
        }

        const TString& GetFnId() const noexcept
        {
            return FnId_;
        };

    protected:
        void SetTimer(const TKey& key, const TTimer::TTimerId& timerId, TInstant deadline, const TTimer::EMergePolicy policy)
        {
            TTimer::TRawKey serializedKey;
            TStringOutput keyStream(serializedKey);
            TCoder<TKey> coder;
            coder.Encode(&keyStream, key);
            SetTimer(
                TTimer{serializedKey, timerId, GetFnId(), static_cast<TTimer::TTimestamp>(deadline.TimeT()), {}},
                policy
            );
        }

        void SetTimer(const TTimer& timer, const TTimer::EMergePolicy policy)
        {
            GetExecutionContext()->SetTimer(timer, policy);
        }

        virtual void OnTimer(const TKey& key, TOutput<TOutputRow>& output, TState& state, const TTimerContext& timerContext) = 0;

        TString FnId_;
};

////////////////////////////////////////////////////////////////////////////////
} // namespace NRoren
