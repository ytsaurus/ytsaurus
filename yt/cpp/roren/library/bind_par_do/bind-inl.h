#ifndef BIND_INL_H_
#error "Direct inclusion of this file is not allowed, include bind.h"
// For the sake of sane code completion.
#include "bind.h"
#endif
#undef BIND_INL_H_

#include <util/generic/function.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename F, typename Arg, typename... Args>
auto BindParDo(F func, Arg firstArg, Args... args)
{
    if constexpr (std::is_same_v<std::decay_t<Arg>, TFnAttributes>) {
        const auto callback = TConstructionSerializableCallback(std::move(func), std::move(args)...);

        using TCallback = decltype(callback);
        using TInputRow = typename std::decay_t<TFunctionArg<TCallback, 0>>;

        if constexpr (std::invocable<TCallback, TInputRow>) {
            using TOutputRow = std::invoke_result_t<TCallback, TInputRow>;
            const auto fixedCallback = TConstructionSerializableCallback(
                [](const TCallback& callback, const TInputRow& input, NRoren::TOutput<TOutputRow>& output) {
                    if constexpr (!std::is_void_v<TOutputRow>) {
                        output.Add(callback(input));
                    } else {
                        Y_UNUSED(output);
                        callback(input);
                    }
                },
                std::move(callback));
            return NRoren::MakeParDo<NPrivate::TFuncParDo<TInputRow, TOutputRow>>(std::move(fixedCallback), std::move(firstArg));
        } else {
            using TArgLast = typename std::decay_t<TFunctionArg<TCallback, 1>>;
            using TOutputRow = typename TArgLast::TRowType;
            if constexpr (std::is_same_v<TOutputRow, TMultiRow>) {
                static_assert(TDependentFalse<F>, "Creating ParDo's with multiple output from is not supported, create class implementing IDoFn<TInputRow, TMultiRow>");
            } else {
                static_assert(std::invocable<TCallback, TInputRow, NRoren::TOutput<TOutputRow>&>, "Such function is not supported");
                return NRoren::MakeParDo<NPrivate::TFuncParDo<TInputRow, TOutputRow>>(std::move(callback), std::move(firstArg));
            }
        }
    } else {
        return BindParDo<F, TFnAttributes, Arg, Args...>(
            std::forward<F>(func),
            TFnAttributes{},
            std::forward<Arg>(firstArg),
            std::forward<Args>(args)...);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <typename TInputRow_, typename TOutputRow_>
TFuncParDo<TInputRow_, TOutputRow_>::TFuncParDo(TParDoFunction fn, TFnAttributes fnAttributes)
    : Fn_(std::move(fn))
    , FnAttributes_(std::move(fnAttributes))
{ }

template <typename TInputRow_, typename TOutputRow_>
void TFuncParDo<TInputRow_, TOutputRow_>::Do(const TInputRow& input, NRoren::TOutput<TOutputRow>& output)
{
    Fn_(input, output);
}

template <typename TInputRow_, typename TOutputRow_>
TFnAttributes TFuncParDo<TInputRow_, TOutputRow_>::GetDefaultAttributes() const {
    return FnAttributes_;
}

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
