#pragma once

#include <yt/yt/core/misc/enum.h>

#include <library/cpp/yt/misc/port.h>

#include <util/generic/hash_set.h>

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

        bool operator==(const TErrorCodeInfo& rhs) const;
    };

    TErrorCodeInfo Get(int code) const;

    THashMap<int, TErrorCodeInfo> GetAll() const;

    void RegisterErrorCode(int code, const TErrorCodeInfo& errorCodeInfo);

    static TString ParseNamespace(const std::type_info& errorCodeEnumTypeInfo);

private:
    THashMap<int, TErrorCodeInfo> CodeToInfo_;
};

TString ToString(const TErrorCodeRegistry::TErrorCodeInfo& errorCodeInfo);

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_ERROR_ENUM(seq) \
    DEFINE_ENUM(EErrorCode, seq); \
    ATTRIBUTE_USED inline const void* ErrorEnum_EErrorCode = [] { \
        for (auto errorCode : ::NYT::TEnumTraits<EErrorCode>::GetDomainValues()) { \
            ::NYT::TErrorCodeRegistry::Get()->RegisterErrorCode( \
                static_cast<int>(errorCode), \
                {::NYT::TErrorCodeRegistry::ParseNamespace(typeid(EErrorCode)), ToString(errorCode)}); \
        } \
        return nullptr; \
    } ()

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
