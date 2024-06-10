#pragma once

#include <yt/cpp/roren/interface/roren.h>

#include <yt/cpp/roren/interface/private/save_loadable_pointer_wrapper.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename F>
auto WithKey(F func);

template <typename TKey, typename TValue>
class TWithKeyTransform
{
public:
    using TFunctionPtr = TKey(*)(const TValue&);

public:
    TWithKeyTransform(TFunctionPtr func)
        : Fn_(MakeIntrusive<TParDoFn>(func))
    { }

    TString GetName() const
    {
        return "WithKey";
    }

    TPCollection<TKV<TKey, TValue>> ApplyTo(const TPCollection<TValue>& pCollection) const
    {
        return pCollection | ParDo(Fn_);
    }

private:
    class TParDoFn : public IDoFn<TValue, TKV<TKey, TValue>>
    {
    public:
        TParDoFn() = default;

        explicit TParDoFn(TFunctionPtr func)
            : Func_(func)
        { }

        void Do(const TValue& input, TOutput<TKV<TKey, TValue>>& output) override
        {
            output.Add({Func_(input), input});
        }

    private:
        TFunctionPtr Func_ = nullptr;

        Y_SAVELOAD_DEFINE_OVERRIDE(NPrivate::SaveLoadablePointer(Func_));
    };

private:
    ::TIntrusivePtr<TParDoFn> Fn_;
};

template <typename F>
auto WithKey(F func)
{
    using TDecayedF = std::decay_t<F>;
    using TFInput = typename std::decay_t<TFunctionArg<TDecayedF, 0>>;
    using TFOutput = std::invoke_result_t<TDecayedF, TFInput>;
    return TWithKeyTransform<TFOutput, TFInput>{func};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
