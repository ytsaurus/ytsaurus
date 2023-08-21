#pragma once

#include <yt/yt/client/table_client/row_base.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

template <template <NTableClient::EValueType Type> class TFunction, class... TArgs>
auto DispatchByDataType(NTableClient::EValueType type, TArgs&&... args);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

#define DISPATCH_BY_TYPE_INL_H_
#include "dispatch_by_type-inl.h"
#undef DISPATCH_BY_TYPE_INL_H_
