#ifndef SERIALIZER_INL_H_
#error "Direct inclusion of this file is not allowed, include serializer.h"
// For the sake of sane code completion.
#include "serializer.h"
#endif
#undef SERIALIZER_INL_H_

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
NTableClient::TTableSchemaPtr GetYsonTableSchema()
{
    return GetYsonTableSchema(New<T>());
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> Deserialize(const NTableClient::TUnversionedRow& row, const NTableClient::TTableSchemaPtr& schema)
{
    auto result = New<T>();
    Deserialize(result, row, schema);
    return result;
}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer
