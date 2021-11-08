#pragma once

#include "private.h"

#include <DataTypes/IDataType.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NYson {

////////////////////////////////////////////////////////////////////////////////

template <class TAST>
void Serialize(const TAST& ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAST*, DB::IAST*>::value>* = nullptr);

template <class TAST>
void Serialize(const TAST* ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAST*, DB::IAST*>::value>* = nullptr);

template <class TAST>
void Serialize(const std::shared_ptr<TAST>& ast, NYson::IYsonConsumer* consumer, std::enable_if_t<std::is_convertible<TAST*, DB::IAST*>::value>* = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

namespace DB {

////////////////////////////////////////////////////////////////////////////////

TString ToString(const DB::DataTypePtr& dataType);

////////////////////////////////////////////////////////////////////////////////

} // namespace DB

#define FORMAT_INL_H_
#include "format-inl.h"
#undef FFORMAT_INL_H_
