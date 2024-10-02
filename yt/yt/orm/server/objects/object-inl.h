#ifndef OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include object.h"
// For the sake of sane code completion.
#include "object.h"
#endif

#include <yt/yt/orm/client/objects/key.h>
#include <yt/yt/orm/client/objects/registry.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T, class = void>
struct THasStaticType
{
    static constexpr bool Value = false;
};

template <class T>
struct THasStaticType<T, decltype(T::Type)>
{
    static constexpr bool Value = true;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TObject::ValidateAs() const
{
    if constexpr (NDetail::THasStaticType<T>::Value) {
        auto expectedType = T::Type;
        auto actualType = GetType();
        if (expectedType != actualType) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidObjectType,
                "Invalid type of object %v: expected %Qv, actual %Qv",
                GetKey(),
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(expectedType),
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(actualType));
        }
    } else {
        if (!dynamic_cast<const T*>(this)) {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectType,
                "Invalid type %Qv of object %v",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(GetType()),
                GetKey());
        }
    }
}

template <class T>
const T* TObject::As() const
{
    ValidateAs<T>();
    return static_cast<const T*>(this);
}

template <class T>
T* TObject::As()
{
    ValidateAs<T>();
    return static_cast<T*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
