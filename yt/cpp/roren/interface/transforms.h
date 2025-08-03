#pragma once

///
/// @file transforms.h
///
/// Roren primitive transforms.
///
/// Primitive transforms are basic blocks for pipelines and composite transforms.
///
/// Users might combine primitive transforms to make their own composite transforms.
/// In order to make new composite transform one should define class with method `ApplyTo`:
///
///     RESULT ApplyTo(const INPUT& input) const;
///
/// where `INPUT` is either `TPCollection` or `TPipeline<...>` and RESULT` is either `TPCollection<...>` or `void`.

#include "fwd.h"
#include "fns.h"
#include "key_value.h"
#include "co_gbk_result.h"
#include "private/attributes.h"
#include "private/combine.h"
#include "private/flatten.h"
#include "private/fn_attributes_ops.h"
#include "private/fwd.h"
#include "private/group_by_key.h"
#include "private/raw_par_do.h"
#include "private/raw_pipeline.h"
#include "private/stateful_par_do.h"
#include "private/stateful_timer_par_do.h"
#include "private/flatten.h"

#include <yt/cpp/roren/library/bind/bind.h>

#include <library/cpp/yt/misc/variant.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/system/type_name.h>

#include <type_traits>
#include <functional>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

///
/// @brief ParDo is a primitive transform similar to Map phase of Map/Shuffle/Reduce
///
/// ParDo transform considers each element in the input TPCollection,
/// performs some processing function (your user code) on that element,
/// and emits zero, one, or multiple elements to an output PCollection.
///
/// @param func contains user code and might be passed in one of the following forms
///
/// Function with signature `TOutput (const T&/T&&)`.
/// Example:
///
///     auto transform = ParDo([] (const TString& in) {
///         return in.size();
///     });
///
/// Function with signature `TOutput (const T&/T&&, TOutput<TOutput>&)`
/// Example:
///
///     auto transform = ParDo([] (const TString& in, TOutput<TString>& out) {
///         if (!in.empty()) {
///             out.Add(in);
///         }
///     });
///
///     auto transform = ParDo([] (TString&& in, TOutput<TString>& out) {
///         if (!in.empty()) {
///             out.Add(std::move(in));
///         }
///     });
///
/// Intrusive pointer to subclass of `IDoFn<T, TOutput>`
/// Example:
///
///     class TMyParDo : public NRoren::IDoFn<TString, int>
///     ...
///     auto transform = ParDo(NYT::New<TMyParDo>());
///
/// @param attributes contains attributes of a function, they will be merged with
/// attributes provided by `func' (values from `attributes' have higher precedence).
///
/// @param Additional binding arguments can be provided for function version:
/// Example:
///
///     TString badId = "42";
///     auto transform = ParDo(
///         [] (const TString& in, TOutput<TString>& out, IExecutionContext*, const TString& badId) {
///             if (in != badId) {
///                 out.Add(in);
///             }
///         },
///         std::move(badId));
template <typename F, typename... TArgs>
auto ParDo(F&& func, TArgs&&... args);

template <NPrivate::CDoFn TFunc>
auto ParDo(NYT::TIntrusivePtr<TFunc> func);

////////////////////////////////////////////////////////////////////////////////

class TGroupByKeyTransform GroupByKey();

////////////////////////////////////////////////////////////////////////////////

template <CCombineFnPtr TFnPtr>
auto CombinePerKey(TFnPtr combineFn);

////////////////////////////////////////////////////////////////////////////////


///
/// Create transform that takes PCollection of any type and does nothing with it.
///
/// Useful in some situations when we want to avoid hanging PCollection nodes.
TNullWriteTransform NullWrite();

////////////////////////////////////////////////////////////////////////////////

template <typename TOutputRow>
class TReadTransform
    : public NPrivate::IWithAttributes
{
public:
    explicit TReadTransform(NPrivate::IRawReadPtr rawRead, const TString& name = "")
        : RawRead_(rawRead)
        , Name_(!name.empty() ? name : "Read")
    { }

    TString GetName() const
    {
        return Name_;
    }

    TPCollection<TOutputRow> ApplyTo(const TPipeline& pipeline) const
    {
        const auto& rawPipeline = NPrivate::GetRawPipeline(pipeline);
        auto transformNode = rawPipeline->AddTransform(RawRead_, {});
        const auto& sinkNodeList = transformNode->GetTaggedSinkNodeList();
        Y_ABORT_UNLESS(sinkNodeList.size() == 1);
        return NPrivate::MakePCollection<TOutputRow>(sinkNodeList[0].second, rawPipeline);
    }

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        RawRead_->SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return RawRead_->GetAttribute(key);
    }

private:
    const NPrivate::IRawReadPtr RawRead_;
    const TString Name_;
};

template <typename T>
TReadTransform<T> DummyRead(const TString& name = "")
{
    return TReadTransform<T>{NYT::New<NPrivate::TRawDummyRead>(NPrivate::MakeRowVtable<T>()), name};
}

////////////////////////////////////////////////////////////////////////////////

template <typename TInputRow>
class TWriteTransform
    : public NPrivate::IWithAttributes
{
public:
    explicit TWriteTransform(NPrivate::IRawWritePtr rawWriteTransform)
        : RawWrite_(std::move(rawWriteTransform))
    { }

    TString GetName() const
    {
        return "Write";
    }

    void ApplyTo(const TPCollection<TInputRow>& pCollection) const
    {
        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollection);
        auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();
        auto transformNode = rawPipeline->AddTransform(RawWrite_, {rawInputNode});
        Y_ABORT_UNLESS(transformNode->GetTaggedSinkNodeList().size() == 0);
    }

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        RawWrite_->SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return RawWrite_->GetAttribute(key);
    }

private:
    const NPrivate::IRawWritePtr RawWrite_;
};

template <typename T>
TWriteTransform<T> DummyWrite()
{
    return TWriteTransform<T>{NYT::New<NPrivate::TRawDummyWriter>(NPrivate::MakeRowVtable<T>())};
}

////////////////////////////////////////////////////////////////////////////////

//
// ParDo
//

template <typename TInput, typename TOutput>
class TParDoTransform
    : public NPrivate::IWithAttributes
{
    static_assert(CRow<TInput>);
    static_assert(CRow<TOutput> || CMultiRow<TOutput>);

public:
    explicit TParDoTransform(NPrivate::IRawParDoPtr rawParDo)
        : RawParDo_(std::move(rawParDo))
    { }

    TString GetName() const
    {
        return "ParDo";
    }

    auto ApplyTo(const TPCollection<TInput>& pCollection) const
    {
        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollection);
        auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();
        auto transformNode = rawPipeline->AddTransform(RawParDo_, {rawInputNode});
        auto taggedSinkNodeList = transformNode->GetTaggedSinkNodeList();

        if constexpr (std::is_same_v<TOutput, TMultiRow>) {
            return NPrivate::MakeMultiPCollection(taggedSinkNodeList, rawPipeline);
        } else if constexpr (std::is_same_v<TOutput, void>) {
            return;
        } else {
            Y_ABORT_UNLESS(taggedSinkNodeList.size() == 1);
            auto rawNode = taggedSinkNodeList[0].second;
            return NPrivate::MakePCollection<TOutput>(rawNode, rawPipeline);
        }
    }

    auto ApplyTo(const TPipeline& pipeline) const
    {
        static_assert(std::is_same_v<TInput, void>);
        const auto& rawPipeline = NPrivate::GetRawPipeline(pipeline);
        auto transformNode = rawPipeline->AddTransform(RawParDo_, {});
        auto taggedSinkNodeList = transformNode->GetTaggedSinkNodeList();

        //if constexpr (std::is_same_v<TOutput, TMultiRow>) {
        //    return NPrivate::MakeMultiPCollection(taggedSinkNodeList, rawPipeline);
        if constexpr (std::is_same_v<TOutput, void>) {
            return;
        } else {
            Y_ABORT_UNLESS(taggedSinkNodeList.size() == 1);
            auto rawNode = taggedSinkNodeList[0].second;
            return NPrivate::MakePCollection<TOutput>(rawNode, rawPipeline);
        }
    }

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        RawParDo_->SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return RawParDo_->GetAttribute(key);
    }

private:
    const NPrivate::IRawParDoPtr RawParDo_;
};

template <NPrivate::CDoFn TFunc>
auto ParDo(NYT::TIntrusivePtr<TFunc> func)
{
    using TInputRow = std::decay_t<typename TFunc::TInputRow>;
    using TOutputRow = typename TFunc::TOutputRow;
    auto rawParDo = NPrivate::MakeRawParDo(std::move(func));
    return TParDoTransform<TInputRow, TOutputRow>(std::move(rawParDo));
}

template <NPrivate::CDoFn F, typename... TArgs>
auto MakeParDo(TArgs&&... args)
{
    return ParDo(NYT::New<F>(std::forward<TArgs>(args)...));
}

template <typename F, typename... TArgs>
auto ParDo(F&& func, TArgs&&... args)
{
    auto bind = BindBack(std::forward<F>(func), std::forward<TArgs>(args)...);
    using TFunctor = std::decay_t<decltype(bind)>;
    using TInputRowArg = TFunctionArg<TFunctor, 0>;
    using TInputRow = TDoFnTemplateArgument<TInputRowArg>;
    return std::invoke(
        NYT::TOverloaded{
            [&] <typename TOutputRow, typename... Ts> (
                std::type_identity<void(TInputRowArg, TOutput<TOutputRow>&, Ts...)>)
            {
                using TDoFn = NPrivate::TFunctorDoFn<TFunctor, TInputRow, TOutputRow>;
                return MakeParDo<TDoFn>(std::move(bind));
            },
            [&] <typename TOutputRow, typename... Ts> (
                std::type_identity<TOutputRow(TInputRowArg, Ts...)>)
                requires (!std::disjunction_v<TIsTemplateBaseOf<TOutput, std::decay_t<Ts>>...>)
            {
                auto wrapped = BindBack(
                    [] (TInputRowArg input, TOutput<TOutputRow>& output, Ts... args, const TFunctor& fn) {
                        if constexpr (std::same_as<TOutputRow, void>) {
                            std::invoke(fn, std::forward<TInputRowArg>(input), std::forward<Ts>(args)...);
                        } else {
                            output.Add(std::invoke(fn, std::forward<TInputRowArg>(input), std::forward<Ts>(args)...));
                        }
                    },
                    std::move(bind));
                using TDoFn = NPrivate::TFunctorDoFn<std::decay_t<decltype(wrapped)>, TInputRow, TOutputRow>;
                return MakeParDo<TDoFn>(std::move(wrapped));
            },
        },
        std::type_identity<TFunctionSignature<TFunctor>>{});
}

////////////////////////////////////////////////////////////////////////////////

//
// StatefulParDo
//

template <typename TInput, typename TOutput, typename TState>
class TStatefulParDoTransform
    : public NPrivate::IWithAttributes
{
public:
    TStatefulParDoTransform(NPrivate::IRawStatefulParDoPtr fn, NPrivate::TRawPStateNodePtr pState)
        : RawStatefulParDo_(std::move(fn))
        , RawPStateNode_(pState)
    { }

    TString GetName() const
    {
        return "StatefulParDo";
    }

    auto ApplyTo(const TPCollection<TInput>& pCollection) const
    {
        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollection);
        auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();
        auto transformNode = rawPipeline->AddTransform(RawStatefulParDo_, {rawInputNode}, RawPStateNode_);
        auto taggedSinkNodeList = transformNode->GetTaggedSinkNodeList();

        if constexpr (CMultiRow<TOutput>) {
            return NPrivate::MakeMultiPCollection(taggedSinkNodeList, rawPipeline);
        } else if constexpr (std::is_same_v<TOutput, void>) {
            return;
        } else {
            Y_ABORT_UNLESS(taggedSinkNodeList.size() == 1);
            auto rawNode = taggedSinkNodeList[0].second;
            return NPrivate::MakePCollection<TOutput>(rawNode, rawPipeline);
        }
    }

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        RawStatefulParDo_->SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return RawStatefulParDo_->GetAttribute(key);
    }


private:
    const NPrivate::IRawStatefulParDoPtr RawStatefulParDo_;
    const NPrivate::TRawPStateNodePtr RawPStateNode_;
};

template <NPrivate::CStatefulDoFn TFunc, typename TKey, typename TState>
auto StatefulParDo(TPState<TKey, TState> pState, NYT::TIntrusivePtr<TFunc> func)
{
    static_assert(
        std::is_same_v<TState, typename TFunc::TState>,
        "Type of PState doesn't match StatefulDoFn");
    using TInputRow = std::decay_t<typename TFunc::TInputRow>;
    using TOutputRow = typename TFunc::TOutputRow;
    static_assert(NTraits::IsTKV<TInputRow>, "Input row of transform must be TKV");
    static_assert(std::is_same_v<typename TInputRow::TKey, TKey>, "Key of input row must match key of PState");
    auto rawFn = NPrivate::MakeRawStatefulParDo(std::move(func));
    auto rawState = NPrivate::GetRawPStateNode(pState);
    return TStatefulParDoTransform<TInputRow, TOutputRow, TState>(rawFn, rawState);
}

template <typename T, typename... TArgs>
auto MakeStatefulParDo(
    TPState<typename std::decay_t<typename T::TInputRow>::TKey, typename T::TState> pState,
    TArgs&&... args)
{
    return StatefulParDo(pState, NYT::New<T>(std::forward<TArgs>(args)...));
}

template <typename TKey, typename TState, typename F, typename... TArgs>
    requires (!NPrivate::CStatefulDoFn<F>)
auto StatefulParDo(TPState<TKey, TState> pState, F&& func, TArgs&&... args)
{
    auto bind = BindBack(std::forward<F>(func), std::forward<TArgs>(args)...);
    using TFunctor = std::decay_t<decltype(bind)>;
    using TInputRow = TDoFnTemplateArgument<TFunctionArg<TFunctor, 0>>;
    using TOutputRow = typename std::decay_t<TFunctionArg<TFunctor, 1>>::TRowType;
    using TDoFn = NPrivate::TFunctorStatefulDoFn<TFunctor, TInputRow, TOutputRow, TState>;
    return MakeStatefulParDo<TDoFn>(pState, std::move(bind));
}

////////////////////////////////////////////////////////////////////////////////

//
// StatefulTimerParDo
//

template <typename TInput, typename TOutput, typename TState>
class TStatefulTimerParDoTransform
{
public:
    TStatefulTimerParDoTransform(NPrivate::IRawStatefulTimerParDoPtr fn, NPrivate::TRawPStateNodePtr pState)
        : RawStatefulTimerParDo_(std::move(fn))
        , RawPStateNode_(pState)
    { }

    TString GetName() const
    {
        return "StatefulTimerParDo";
    }

    auto ApplyTo(const TPCollection<TInput>& pCollection) const
    {
        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollection);
        auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();
        auto transformNode = rawPipeline->AddTransform(RawStatefulTimerParDo_, {rawInputNode}, RawPStateNode_);
        auto taggedSinkNodeList = transformNode->GetTaggedSinkNodeList();

        if constexpr (CMultiRow<TOutput>) {
            return NPrivate::MakeMultiPCollection(taggedSinkNodeList, rawPipeline);
        } else if constexpr (std::is_same_v<TOutput, void>) {
            return;
        } else {
            Y_ABORT_UNLESS(taggedSinkNodeList.size() == 1);
            auto rawNode = taggedSinkNodeList[0].second;
            return NPrivate::MakePCollection<TOutput>(rawNode, rawPipeline);
        }
    }

private:
    const NPrivate::IRawStatefulTimerParDoPtr RawStatefulTimerParDo_;
    const NPrivate::TRawPStateNodePtr RawPStateNode_;
};

template <typename TFn, typename TKey, typename TState>
auto StatefulTimerParDo(TPState<TKey, TState> pState, TFn fn, const TFnAttributes& attributes = {})
{
    using TDecayedF = std::decay_t<TFn>;
    if constexpr (NPrivate::CIntrusivePtr<TDecayedF>) {
        static_assert(std::is_same_v<TState, typename TDecayedF::TUnderlying::TState>, "Type of PState doesn't match StatefulTimerDoFn");

        using TInput = typename TDecayedF::TUnderlying::TInputRow;
        using TOutput = typename TDecayedF::TUnderlying::TOutputRow;
        auto rawFn = NPrivate::MakeRawStatefulTimerParDo(fn, attributes);
        auto rawState = NPrivate::GetRawPStateNode(pState);
        return TStatefulTimerParDoTransform<TInput, TOutput, TState>{rawFn, rawState};
    } else {
        static_assert(TDependentFalse<TFn>, "not supported yet");
    }
}

template <typename T, typename... Args>
auto MakeStatefulTimerParDo(TPState<typename T::TInputRow::TKey, typename T::TState> pState, Args... args)
{
    return StatefulTimerParDo(pState, NYT::New<T>(args...));
}

////////////////////////////////////////////////////////////////////////////////

//
// GroupByKey
//

class TGroupByKeyTransform
    : public NPrivate::IWithAttributes
{
public:
    TGroupByKeyTransform() = default;

    TString GetName() const
    {
        return "GroupByKey";
    }

    template <typename TKey, typename TValue>
    TPCollection<TKV<TKey, TInputPtr<TValue>>> ApplyTo(const TPCollection<TKV<TKey, TValue>>& pCollection) const
    {
        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollection);
        auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();
        auto rawTransform = NPrivate::MakeRawGroupByKey<TKey, TValue>();
        MergeAttributes(*rawTransform, Attributes_);
        auto transformNode = rawPipeline->AddTransform(rawTransform, {rawInputNode});

        const auto& taggedSinkNodeList = transformNode->GetTaggedSinkNodeList();
        Y_ABORT_UNLESS(taggedSinkNodeList.size() == 1);
        auto rawOutputNode = taggedSinkNodeList[0].second;
        return MakePCollection<TKV<TKey, TInputPtr<TValue>>>(rawOutputNode, rawPipeline);
    }

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        Attributes_.SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return Attributes_.GetAttribute(key);
    }

private:
    NPrivate::TAttributes Attributes_;
};

TGroupByKeyTransform GroupByKey();

////////////////////////////////////////////////////////////////////////////////

//
// CombinePerKey
//

template <typename TCombineFn>
class TCombinePerKeyTransform
    : public NPrivate::IWithAttributes
{
public:
    using TCombineInput = typename TCombineFn::TInputRow;
    using TCombineOutput = typename TCombineFn::TOutputRow;

public:
    explicit TCombinePerKeyTransform(NYT::TIntrusivePtr<TCombineFn> combineFn)
        : CombineFn_(std::move(combineFn))
    { }

    TString GetName() const
    {
        return "CombinePerKey";
    }

    template <typename TKey>
    TPCollection<TKV<TKey, TCombineOutput>> ApplyTo(const TPCollection<TKV<TKey, TCombineInput>>& pCollection) const
    {
        using TInputRow = TKV<TKey, TCombineInput>;
        using TOutputRow = TKV<TKey, TCombineOutput>;

        auto rawCombine = NPrivate::MakeRawCombine<TCombineFn>(
            NPrivate::ERawTransformType::CombinePerKey,
            CombineFn_,
            NPrivate::MakeRowVtable<TInputRow>(),
            NPrivate::MakeRowVtable<TOutputRow>()
        );
        NPrivate::MergeAttributes(*rawCombine, Attributes_);

        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollection);
        auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();

        auto transformNode = rawPipeline->AddTransform(rawCombine, {rawInputNode});
        const auto& taggedSinkNodeList = transformNode->GetTaggedSinkNodeList();

        Y_ABORT_UNLESS(taggedSinkNodeList.size() == 1);
        const auto& rawOutputNode = taggedSinkNodeList[0].second;

        return MakePCollection<TOutputRow>(rawOutputNode, rawPipeline);
    }

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        Attributes_.SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return Attributes_.GetAttribute(key);
    }

    const NYT::TIntrusivePtr<TCombineFn> CombineFn_;
    NPrivate::TAttributes Attributes_;
};

template <CCombineFnPtr TFnPtr>
auto CombinePerKey(TFnPtr combineFn)
{
    return TCombinePerKeyTransform{std::move(combineFn)};
}

template <typename F, typename... TArgs>
    requires (!CCombineFnPtr<F>)
auto CombinePerKey(F&& func, TArgs&&... args)
{
    auto bind = BindBack(std::forward<F>(func), std::forward<TArgs>(args)...);
    using TFunctor = std::decay_t<decltype(bind)>;
    using TRow = std::decay_t<std::remove_pointer_t<TFunctionArg<TFunctor, 0>>>;
    static_assert(std::same_as<TFunctionArg<TFunctor, 0>, TRow*>);
    static_assert(std::same_as<TFunctionArg<TFunctor, 1>, const TRow&>);
    return CombinePerKey(NYT::New<TFunctorCombineFn<TFunctor, TRow>>(std::move(bind)));
}

////////////////////////////////////////////////////////////////////////////////

class TCoGroupByKeyTransform
    : public NPrivate::IWithAttributes
{
public:
    template <typename>
    using TOutputRow = TCoGbkResult;

public:
    TCoGroupByKeyTransform() = default;

    TString GetName() const;

    TPCollection<TCoGbkResult> ApplyTo(const TMultiPCollection& multiPCollection) const;

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        Attributes_.SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return Attributes_.GetAttribute(key);
    }

private:
    NPrivate::TAttributes Attributes_;
};


TCoGroupByKeyTransform CoGroupByKey();

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Flatten transform
///
/// Flatten transform unites multiple PCollections of the same type into single PCollection.
///
/// PCollections MUST belong to the same pipeline.
/// At least one input PCollection MUST be provided.
class TFlattenTransform
    : public NPrivate::IWithAttributes
{
public:
    TString GetName() const
    {
        return "Flatten";
    }

    template <typename TRow>
    TPCollection<TRow> ApplyTo(const std::vector<TPCollection<TRow>>& pCollectionList) const
    {
        Y_ABORT_UNLESS(pCollectionList.size() > 0, "Cannot flatten empty list of pCollection");
        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollectionList.front());
        std::vector<NPrivate::TPCollectionNode*> pCollectionNodeList;
        for (const auto& pCollection : pCollectionList) {
            const auto& curRawPipeline = NPrivate::GetRawPipeline(pCollection);
            Y_ABORT_UNLESS(curRawPipeline.Get() == rawPipeline.Get(), "Cannot flatten pCollections belonging to different pipelines");

            auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();
            pCollectionNodeList.push_back(rawInputNode);
        }
        auto rawFlatten = NPrivate::MakeRawFlatten<TRow>(NPrivate::MakeRowVtable<TRow>(), ssize(pCollectionList));

        auto transformNode = rawPipeline->AddTransform(rawFlatten, pCollectionNodeList);
        const auto& taggedSinkNodeList = transformNode->GetTaggedSinkNodeList();

        Y_ABORT_UNLESS(taggedSinkNodeList.size() == 1);
        const auto& rawOutputNode = taggedSinkNodeList[0].second;

        NPrivate::MergeAttributes(*transformNode->GetRawTransform(), Attributes_);

        return MakePCollection<TRow>(rawOutputNode, rawPipeline);
    }

    template <typename TRow>
    TPCollection<TRow> ApplyTo(const TPCollection<TRow>& pCollection) const
    {
        return ApplyTo(std::vector{pCollection});
    }

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        Attributes_.SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return Attributes_.GetAttribute(key);
    }

private:
    NPrivate::TAttributes Attributes_;
};

TFlattenTransform Flatten();

/// TODO: remove in favour of previous `Flatten` definition.
template <typename TRow>
TPCollection<TRow> Flatten(const std::vector<TPCollection<TRow>>& pCollectionList)
{
    return pCollectionList | Flatten();
}

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Generic transform
///
/// Is meant to returned by functions creating composite transforms providing
/// possibility to hide implementation details in `.cpp` file.
///
/// Example of usage:
///
///     // header
///     TTransform<int, int> MyTransform();
///
///     // source
///     // N.B. `TMyTransform` is hidden inside source file and doesn't pollute header.
///     class TMyTransform
///     {
///     public:
///         ...
///         TPCollection<int> ApplyTo(const TPCollection<int>& pCollection) const { ... }
///     };
///
///     TTransform<int, int> MyTransform()
///     {
///         return TMyTransform{ ... }
///     }
template <typename TInputRow, typename... TOutputRows>
class TTransform
{
    template <typename TFirst_, typename... TTypes>
    struct TTypesHolder
    {
        using TFirst = TFirst_;
    };
public:
    using TArgument = std::conditional_t<std::is_same_v<TInputRow, void>, TPipeline, TPCollection<TInputRow>>;
    using TResult = std::conditional_t<
        std::is_same_v<TTypesHolder<TOutputRows...>, TTypesHolder<void>>,
        void,
        std::conditional_t<
            sizeof...(TOutputRows) == 1,
            TPCollection<typename TTypesHolder<TOutputRows...>::TFirst>,
            TPCollections<TOutputRows...>>>;

public:
    template <typename T>
        requires CApplicableTo<T, TArgument>
    TTransform(T t)
        : Name_(t.GetName())
        , Applier_([t=t] (const TArgument& input) {
            return t.ApplyTo(input);
        })
    { }

    template <typename T>
        requires std::is_convertible_v<T, std::function<TResult(const TArgument&)>>
    TTransform(TString name, T&& applier)
        : Name_(std::move(name))
        , Applier_(std::forward<T>(applier))
    { }

    template <typename T>
        requires std::is_convertible_v<T, std::function<TResult(const TArgument&)>>
    TTransform(T&& applier)
        : Name_("UserDefinedTransform")
        , Applier_(std::forward<T>(applier))
    { }

    TString GetName() const
    {
        return Name_;
    }

    TResult ApplyTo(const TArgument& pCollection) const
    {
        return Applier_(pCollection);
    }

private:
    const TString Name_;
    const std::function<TResult(const TArgument&)> Applier_;
};

////////////////////////////////////////////////////////////////////////////////

/// Easy way to build transform from lambda function.
template <typename TFunction>
class TGenericTransform
{
public:
    template <typename TFunctionArg>
    explicit TGenericTransform(const TString& name, TFunctionArg&& function)
        : Name_(name)
        , Function_(std::forward<TFunctionArg>(function))
    { }

    TString GetName() const
    {
        return Name_;
    }

    auto ApplyTo(const auto& pCollection) const
    {
        return Function_(pCollection);
    }

private:
    TString Name_;
    TFunction Function_;
};

template <typename TFunctionArg>
TGenericTransform(const TString& name, TFunctionArg&& function) -> TGenericTransform<std::remove_cvref_t<TFunctionArg>>;

////////////////////////////////////////////////////////////////////////////////

class TNullWriteTransform
{
public:
    TString GetName() const
    {
        return "NullWrite";
    }

    template <typename T>
    void ApplyTo(const TPCollection<T>& pCollection) const
    {
        return pCollection | ParDo([] (const T&, TOutput<void>&) {
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
