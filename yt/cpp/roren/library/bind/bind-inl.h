#ifndef BIND_INL_H_
#error "Direct inclusion of this file is not allowed, include bind.h"
// For the sake of sane code completion.
#include "bind.h"
#endif

#include <util/generic/function.h>
#include <util/ysaveload.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TFunc, typename... TArgs>
class TSerializableLambda
{
public:
    using TResult = std::invoke_result_t<TFunc, TArgs...>;
    using TSignature = TResult(TArgs...);

    template <typename... TInvokeArgs>
    constexpr TResult operator()(TInvokeArgs&&... args) const
    {
        return std::invoke(TFunc{}, std::forward<TInvokeArgs>(args)...);
    }

    Y_SAVELOAD_DEFINE();
};

template <typename TResult, typename... TArgs>
class TSerializableFnPointer
{
    using TFnPtr = TResult(*)(TArgs...);

public:
    using TSignature = TResult(TArgs...);

    TSerializableFnPointer(TFnPtr fn = nullptr)
        : Fn_(fn)
    { }

    template <typename... TInvokeArgs>
    constexpr TResult operator()(TInvokeArgs&&... args) const
    {
        return std::invoke(Fn_, std::forward<TInvokeArgs>(args)...);
    }

    void Save(IOutputStream* output) const
    {
        ::Save(output, reinterpret_cast<uintptr_t>(Fn_));
    }

    void Load(IInputStream* input)
    {
        uintptr_t fn = 0;
        ::Load(input, fn);
        Fn_ = reinterpret_cast<TFnPtr>(fn);
    }

private:
    TFnPtr Fn_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept CSerializableFunctor = std::is_function_v<TFunctionSignature<T>> &&
    requires (const T& cref, T& ref, IOutputStream* out, IInputStream* in) {
        { cref.Save(out) } -> std::same_as<void>;
        { ref.Load(in) } -> std::same_as<void>;
    };

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename F>
decltype(auto) WrapToSerializableFunctor(F&& fn)
{
    if constexpr (std::is_pointer_v<std::decay_t<F>>) {
        return NPrivate::TSerializableFnPointer(fn);
    } else if constexpr (requires { { +fn }; }) {
        return [] <typename TResult, typename... TArgs> (TResult(*)(TArgs...)) {
            return NPrivate::TSerializableLambda<std::decay_t<F>, TArgs...>();
        } (+fn);
    } else if constexpr (NPrivate::CSerializableFunctor<std::decay_t<F>>) {
        return std::forward<F>(fn);
    } else {
        static_assert(
            TDependentFalse<F>,
            "Given function can't be wrapped to serializable functor.");
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TFunc, typename TBindings, typename... TArgs>
class TBindBackImpl
{
public:
    using TResult = TFunctionResult<TFunc>;
    using TSignature = TResult(TArgs...);

    constexpr TBindBackImpl() = default;

    template <typename F, typename... TBindingArgs>
    constexpr TBindBackImpl(F&& func, TBindingArgs&&... args)
        : Func_(std::forward<F>(func))
        , Bindings_(std::forward<TBindingArgs>(args)...)
    { }

    template <typename... TInvokeArgs>
    constexpr TResult operator()(TInvokeArgs&&... args) const
    {
        static_assert(sizeof...(TInvokeArgs) == sizeof...(TArgs));
        return std::apply(
            [&] (const auto&... boundArgs) -> decltype(auto) {
                return std::invoke(
                    Func_,
                    std::forward<TInvokeArgs>(args)...,
                    boundArgs...);
            },
            Bindings_);
    }

    Y_SAVELOAD_DEFINE(Func_, Bindings_);

private:
    TFunc Func_;
    TBindings Bindings_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename F, typename... TBindings>
auto BindBack(F&& fn, TBindings&&... bindings)
{
    using TFunctor = std::decay_t<decltype(WrapToSerializableFunctor(fn))>;
    using TBindingsTuple = std::tuple<std::decay_t<TBindings>...>;
    return [&] <size_t... Idxs> (std::index_sequence<Idxs...>) {
        static_assert(
            std::is_invocable_v<
                TFunctionSignature<TFunctor>,
                TFunctionArg<TFunctor, Idxs>...,
                const std::decay_t<TBindings>&...>,
            "Binding arguments mismatch function signature.");
        return NPrivate::TBindBackImpl<TFunctor, TBindingsTuple, TFunctionArg<TFunctor, Idxs>...>(
            WrapToSerializableFunctor(std::forward<F>(fn)),
            std::forward<TBindings>(bindings)...);
    } (std::make_index_sequence<TFunctionArgs<TFunctor>::Length - sizeof...(TBindings)>{});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

namespace NPrivate {

template <typename TResult, typename... TArgs>
struct TFuncInfo<NRoren::NPrivate::TSerializableFnPointer<TResult, TArgs...>>
{
    using TSignature = typename NRoren::NPrivate::TSerializableFnPointer<TResult, TArgs...>::TSignature;
};

template <typename TFunc, typename... TArgs>
struct TFuncInfo<NRoren::NPrivate::TSerializableLambda<TFunc, TArgs...>>
{
    using TSignature = typename NRoren::NPrivate::TSerializableLambda<TFunc, TArgs...>::TSignature;
};

template <typename TFunc, typename TBindings, typename... TArgs>
struct TFuncInfo<NRoren::NPrivate::TBindBackImpl<TFunc, TBindings, TArgs...>>
{
    using TSignature = typename NRoren::NPrivate::TBindBackImpl<TFunc, TBindings, TArgs...>::TSignature;
};

} // namespace NPrivate
