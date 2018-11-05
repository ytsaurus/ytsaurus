#pragma once

#include <util/generic/string.h>

#include <vector>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

/// Represents part of table (or concatenation of tables)
/// to be processed by single scan job in query engine

struct TTablePart
{
    /// Serialized job specification.
    TString JobSpec;

    /// Data estimates
    size_t DataWeight = 0;
    size_t RowCount = 0;
};

using TTablePartList = std::vector<TTablePart>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
