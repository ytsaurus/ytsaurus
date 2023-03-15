#pragma once

#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/transforms.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

#include <optional>
#include <string>
#include <tuple>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename TInternalParDo, typename... TArgs>
class TEasyInitParDo : public IDoFn<typename TInternalParDo::TInputRow, typename TInternalParDo::TOutputRow>
{
public:
    using TInputRow = typename TInternalParDo::TInputRow;
    using TOutputRow = typename TInternalParDo::TOutputRow;
    using TImpl = TInternalParDo;

    TEasyInitParDo() = default;

    TEasyInitParDo(const TArgs&... args)
    {
        SerializedArgs_ = Serialize(args...);
        ParDo_.emplace(args...);
    }

    void Save(IOutputStream* s) const override
    {
        s->Write(SerializedArgs_);
    }

    void Load(IInputStream* s) override
    {
        std::tuple args{Deserialize<TArgs>(s)...};
        auto constructPardo = [&](const TArgs&... args) { ParDo_.emplace(args...); };
        std::apply(constructPardo, args);
        SerializedArgs_ = std::apply(Serialize, args);
    }

    void Start(TOutput<TOutputRow>& output) override {
        ParDo_->SetExecutionContext(IDoFn<TInputRow, TOutputRow>::GetExecutionContext());
        ParDo_->Start(output);
    }

    void Do(const TInputRow& input, TOutput<TOutputRow>& output) override
    {
        ParDo_->Do(input, output);
    }

    void Finish(TOutput<TOutputRow>& output) override
    {
        ParDo_->Finish(output);
    }

private:
    template<typename T>
    static T Deserialize(IInputStream* s)
    {
        T value;
        ::Load(s, value);
        return value;
    }

    static std::string Serialize(const TArgs&... args)
    {
        TStringStream ss;
        ::SaveMany(&ss, args...);
        return ss.Str();
    }

    std::optional<TInternalParDo> ParDo_;
    std::string SerializedArgs_;
};

template <typename TParDo, typename... TArgs>
auto MakeEasyInitParDo(const TArgs&... args)
{
    return MakeParDo<TEasyInitParDo<TParDo, TArgs...>>(args...);
}

#define DEFINE_EASY_INIT_PAR_DO(pardo, ...)                        \
    class pardo##Impl {                                            \
    public:                                                        \
        class pardo;                                               \
    };                                                             \
    using pardo = ::NRoren::TEasyInitParDo<pardo##Impl::pardo, __VA_ARGS__>; \
                                                                   \
    class pardo##Impl::pardo

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
