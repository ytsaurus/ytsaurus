#pragma once

#ifndef SIMPLE_EXTERNAL_STATE_MANAGER_INL_H_
    #error "Direct inclusion of this file is not allowed, include simple_external_state_manager.h"
    // For the sake of sane code completion.
    #include "simple_external_state_manager.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T TSimpleExternalState::GetColumnValue(TStringBuf columnName) const
{
    return NTableClient::FromUnversionedValue<T>(GetColumn(columnName));
}

template <class T>
T TSimpleExternalState::GetColumnValue(int columnId) const
{
    return NTableClient::FromUnversionedValue<T>(GetColumn(columnId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
