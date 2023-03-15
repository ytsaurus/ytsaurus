#pragma once

#include <yt/cpp/roren/interface/roren.h>

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

    TPCollection<TKV<TKey, TValue>> ApplyTo(const TPCollection<TValue>& pCollection) const
    {
        return pCollection | ParDo(Fn_);
    }

private:
    class TParDoFn : public IDoFn<TValue, TKV<TKey, TValue>>
    {
    public:
        TParDoFn() = default;

        TParDoFn(TFunctionPtr func)
            : Func_(func)
        { }

        virtual void Do(const TValue& input, TOutput<TKV<TKey, TValue>>& output)
        {
            output.Add({Func_(input), input});
        }

        virtual void Save(IOutputStream* out) const
        {
            ::Save(out, reinterpret_cast<ui64>(Func_));
        }

        virtual void Load(IInputStream* in)
        {
            ui64 ptr;
            ::Load(in, ptr);
            Func_ = reinterpret_cast<TFunctionPtr>(ptr);
        }

    private:
        TFunctionPtr Func_ = nullptr;
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
