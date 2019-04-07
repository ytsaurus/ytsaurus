#pragma once

#include <util/generic/string.h>

#include <vector>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

/// Represents part of table (or concatenation of tables)
/// to be processed by single scan job in query engine

struct TTablePart
{
    /// Base64 encoded proto subquery specification.
    TString SubquerySpec;

    /// Data estimates
    size_t DataWeight = 0;
    size_t RowCount = 0;
};

using TTablePartList = std::vector<TTablePart>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
