#include "mixin.h"

namespace NRoren {

template <typename TDerived>
class TUserConfigMixin : public TConfigExtensionMixin<"extension", NPrivate::TRorenInternalPluginTag<TDerived>>
{
public:
    using TMixin = TConfigExtensionMixin<"extension", NPrivate::TRorenInternalPluginTag<TDerived>>;

    template <typename T>
    NYT::TIntrusivePtr<T> GetUserConfig()
    {
        return TMixin::template Get<T>();
    }
};

} // namespace NRoren

#define REGISTER_USER_CONFIG_EXTENSION(TConfig, Impl) \
    REGISTER_CONFIG_EXTENSION(::NRoren::TUserConfigMixin<TConfig>, Impl)
