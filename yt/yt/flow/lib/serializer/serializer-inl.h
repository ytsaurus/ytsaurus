#ifndef SERIALIZER_INL_H_
#error "Direct inclusion of this file is not allowed, include serializer.h"
// For the sake of sane code completion.
#include "serializer.h"
#endif
#undef SERIALIZER_INL_H_

namespace NYT::NFlow::NYsonSerializer {

////////////////////////////////////////////////////////////////////////////////

template <class TYsonStruct>
TIntrusivePtr<TYsonStruct> Deserialize(
    const NTableClient::TUnversionedRow& row,
    const NTableClient::TLogicalTypePtr& logicalType)
{
    auto ysonStruct = New<TYsonStruct>();
    Deserialize(ysonStruct, row, logicalType);
    return ysonStruct;
}

template <class TYsonStruct>
TIntrusivePtr<TYsonStruct> Deserialize(
    const NTableClient::TUnversionedRow& row,
    const NTableClient::TTableSchemaPtr& schema)
{
    auto ysonStruct = New<TYsonStruct>();
    Deserialize(ysonStruct, row, schema);
    return ysonStruct;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYsonSerializer
