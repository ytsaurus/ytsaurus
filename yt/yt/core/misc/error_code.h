#pragma once

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/enum.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TErrorCodeRegistry
{
public:
    static TErrorCodeRegistry* Get();

    struct TErrorCodeInfo
    {
        TString Namespace;
        TString Name;

        bool operator==(const TErrorCodeInfo& rhs) const = default;
    };

    TErrorCodeInfo Get(int code);

    void RegisterErrorCode(int code, const TErrorCodeInfo& errorCodeInfo);

    static TString ParseNamespace(const std::type_info& errorCodeEnumTypeInfo);

private:
    THashMap<int, TErrorCodeInfo> CodeToInfo_;
};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_ERROR_ENUM(name, seq) \
    DEFINE_ENUM(name, seq); \
    ATTRIBUTE_USED inline const void* ErrorEnum_##name = [] { \
        for (auto errorCode : ::NYT::TEnumTraits<name>::GetDomainValues()) { \
            ::NYT::TErrorCodeRegistry::Get()->RegisterErrorCode( \
                static_cast<int>(errorCode), \
                {::NYT::TErrorCodeRegistry::ParseNamespace(typeid(name)), ToString(errorCode)}); \
        } \
        return nullptr; \
    } ()

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
