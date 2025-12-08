#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

i64 GetInt64(const NTableClient::TUnversionedValue& value);
ui64 GetUint64(const NTableClient::TUnversionedValue& value);
double GetDouble(const NTableClient::TUnversionedValue& value);
bool GetBoolean(const NTableClient::TUnversionedValue& value);
TString GetString(const NTableClient::TUnversionedValue& value);
NYTree::INodePtr GetAny(const NTableClient::TUnversionedValue& value);
NYTree::INodePtr GetComposite(const NTableClient::TUnversionedValue& value);
bool IsNull(const NTableClient::TUnversionedValue& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
