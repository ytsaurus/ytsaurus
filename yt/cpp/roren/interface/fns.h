#pragma once

#include "execution_context.h" // IWYU pragma: keep (clangd bug, required for SetExecutionContext)
#include "fwd.h"
#include "input.h"
#include "output.h"
#include "row.h"
#include "private/concepts.h" // IWYU pragma: keep  (clangd bug, required for concepts)
#include "private/raw_state_store.h"

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
        Y_VERIFY(Context_);
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

class TDoFnAttributes
{
public:
    /// @brief Name of a function. Used in debug and info purposes.
    std::optional<TString> Name;

public:
    /// @brief Merge attributes. Non nullopt values from `other` object will overwrite values from this object.
    void Merge(const TDoFnAttributes& other);
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

    virtual TDoFnAttributes GetDefaultAttributes() const
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
};

template <CRow TInput_, typename TOutput_>
class IBatchDoFn
    : public IFnBase
{
public:
    using TInputRow = TInput_;
    using TOutputRow = TOutput_;

public:
    std::vector<TDynamicTypeTag> GetOutputTags() const
    {
        return {TTypeTag<TOutputRow>("do-fn-output")};
    }

    virtual void Start(TOutput<TOutputRow>&)
    { }
    virtual void Do(std::span<const TInputRow> input, TOutput<TOutputRow>& output) = 0;
    virtual void Finish(TOutput<TOutputRow>&)
    { }
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
    static_assert(sizeof(TStateStore<TKey, TState>) == sizeof(NPrivate::IRawStateStore));
};

////////////////////////////////////////////////////////////////////////////////

template <typename TInputRow_, typename TOutputRow_, typename TState_>
class IStatefulDoFn
    : public IFnBase
{
public:
    static_assert(NTraits::TIsTKV<TInputRow_>::value);
    using TInputRow = TInputRow_;
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
};

template <typename TInputRow_, typename TState_>
class IStatefulDoFn<TInputRow_, TMultiRow, TState_>
    : public IFnBase
{
public:
    static_assert(NTraits::TIsTKV<TInputRow_>::value);
    using TInputRow = TInputRow_;
    using TOutputRow = TMultiRow;
    using TState = TState_;

public:
    virtual std::vector<TDynamicTypeTag> GetOutputTags() const = 0;

    virtual void Start(TOutput<TOutputRow>& /*output*/)
    { }

    virtual void Do(const TInputRow& input, TOutput<TOutputRow>& output, TState& state) = 0;

    virtual void Finish(TStateStore<typename TInputRow::TKey, TState>& /*stateMap*/)
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

} // namespace NRoren
