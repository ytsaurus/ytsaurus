#ifndef SERIALIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include serialize.h"
// For the sake of sane code completion.
#include "serialize.h"
#endif

#include "scalar_attribute_traits.h"

#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void OrmFromUnversionedValue(
    T* value,
    NYT::NTableClient::TUnversionedValue unversionedValue,
    bool loadsNullAsTyped)
{
    if (unversionedValue.Type == NTableClient::EValueType::Null && loadsNullAsTyped) {
        *value = TScalarAttributeTraits<T>::GetDefaultValue();
    } else {
        FromUnversionedValue(value, std::move(unversionedValue));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
