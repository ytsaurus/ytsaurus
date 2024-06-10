#pragma once

#include <yt/yt/core/ytree/yson_struct.h>
#include <util/system/compiler.h>

namespace NRoren {

namespace NPrivate {

template <typename T = void>
struct TRorenInternalPluginTag {};

template <size_t N>
struct TCompileTimeString
{
    char Data[N] = {};

    consteval TCompileTimeString(const char (&data)[N])
    {
        std::copy_n(data, N, Data);
    }
};

} // namespace NPrivate

template <NPrivate::TCompileTimeString Name, typename TTag>
class TConfigExtensionMixin : public virtual NYT::NYTree::TYsonStruct
{
public:
    using TMixin = TConfigExtensionMixin;

    Y_WEAK static NYT::NYTree::TYsonStructPtr NewExtensionInstance()
    {
        return nullptr;
    }

    template <typename T = NYT::NYTree::TYsonStruct>
    NYT::TIntrusivePtr<T> Get()
    {
        return NYT::DynamicPointerCast<T>(Extension_);
    }

    REGISTER_YSON_STRUCT(TConfigExtensionMixin);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter(Name.Data, &TThis::Extension_).DefaultCtor(TThis::NewExtensionInstance);
    }
private:
    NYT::NYTree::TYsonStructPtr Extension_;
};

} // namespace NRoren

#define REGISTER_CONFIG_EXTENSION(Mixin, Impl)                                \
    template <>                                                               \
    auto Mixin::TMixin::NewExtensionInstance() -> NYT::NYTree::TYsonStructPtr \
    {                                                                         \
        return NYT::New<Impl>();                                              \
    }
