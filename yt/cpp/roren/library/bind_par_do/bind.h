#pragma once

#include <yt/cpp/roren/library/construction_serializable/construction_serializable.h>
#include <yt/cpp/roren/library/save_load_wrapper/save_load_serializable_wrapper.h>
#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/transforms.h>

#include <functional>
#include <optional>
#include <tuple>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename F, typename Arg, typename... Args>
auto BindParDo(F func, Arg firstArg, Args... args);

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <typename TInputRow_, typename TOutputRow_>
class TFuncParDo : public IDoFn<TInputRow_, TOutputRow_>
{
    using TParDoFunction = TConstructionSerializableCallback<void(const TInputRow_&, NRoren::TOutput<TOutputRow_>&)>;
public:
    using TInputRow = TInputRow_;
    using TOutputRow = TOutputRow_;

    TFuncParDo() = default;

    TFuncParDo(TParDoFunction fn, TFnAttributes fnAttributes);

    void Do(const TInputRow& input, NRoren::TOutput<TOutputRow>& output) override;

    TFnAttributes GetDefaultAttributes() const override;

    Y_SAVELOAD_DEFINE_OVERRIDE(Fn_, FnAttributes_);
private:
    TParDoFunction Fn_;
    TFnAttributes FnAttributes_;
};

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

#define BIND_INL_H_
#include "bind-inl.h"
#undef BIND_INL_H_
