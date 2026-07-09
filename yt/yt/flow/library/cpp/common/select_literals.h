#pragma once

#include "key.h"
#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <string>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Renders a single #TUnversionedValue as a literal usable inside a YT-QL
//! ``SELECT``. Any-typed values are wrapped in ``yson_string_to_any(...)``.
std::string FormatValueAsSelectLiteral(const NTableClient::TUnversionedValue& value);

//! Renders |key| (which must be a prefix of |schema|) as a parenthesized
//! comma-separated tuple of SELECT literals.
std::string BuildLiteralTuple(const TKey& key, const NTableClient::TTableSchema& schema);

//! Renders the first |count| column names of |schema| as a parenthesized
//! comma-separated tuple.
std::string BuildColumnTuple(const NTableClient::TTableSchema& schema, int count);

//! Renders every column of |schema| as a parenthesized comma-separated tuple.
std::string BuildColumnTuple(const NTableClient::TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
