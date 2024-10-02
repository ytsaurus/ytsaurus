#pragma once

#include <yt/yt/client/table_client/unversioned_value.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void OrmFromUnversionedValue(
    T* value,
    NYT::NTableClient::TUnversionedValue unversionedValue,
    bool loadsNullAsTyped);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
