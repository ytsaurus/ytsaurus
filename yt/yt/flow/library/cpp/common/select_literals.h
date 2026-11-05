#pragma once

#include "key.h"
#include "public.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/yson/string.h>

#include <string>
#include <vector>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Renders a single #TUnversionedValue as a literal usable inside a YT-QL
//! ``SELECT``. Any-typed values are wrapped in ``yson_string_to_any(...)``.
//! Prefer #TSelectPlaceholders: the query text stays constant and the YT query
//! library owns quoting of the values.
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

//! Collects named values of a parameterized YT-QL query, to be passed via
//! #NApi::TSelectRowsOptions::PlaceholderValues instead of inlining literals.
class TSelectPlaceholders
{
public:
    //! Binds |value| to placeholder |name| and returns the expression referencing it.
    std::string Bind(const std::string& name, const NTableClient::TUnversionedValue& value);

    //! Binds |key| (a prefix of |schema|) and returns the whole comparison
    //! clause against the columns it spans, e.g.
    //! ``(hash,word) >= ({lower_0},{lower_1})``.
    std::string BindKeyBound(
        const NTableClient::TTableSchema& schema,
        TStringBuf op,
        const std::string& name,
        const TKey& key);

    NYson::TYsonString Build() const;

private:
    std::vector<std::pair<std::string, NTableClient::TUnversionedOwningValue>> Values_;

    //! Binds the values of |key| to placeholders |name|_0, |name|_1, ... and
    //! returns their parenthesized comma-separated tuple.
    std::string BindTuple(const std::string& name, const TKey& key);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
