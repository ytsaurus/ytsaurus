#pragma once

#include "private.h"

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TTable
    : public TRefCounted
    , public NChunkClient::TUserObject
{
    NTableClient::TTableSchemaPtr Schema;
    NTableClient::TComparator Comparator;
    //! Operand index according to JOIN clause (if any):
    //! - SELECT * FROM AAA: AAA.TableIndex = 0.
    //! - SELECT * FROM AAA JOIN BBB: AAA.TableIndex = 0, BBB.TableIndex = 1.
    //! If operand consists of several tables (like in concat* case), all of them share
    //! same operand index.
    //! NB: Currently, CH handles multi-JOIN as a left-associative sequence of two-operand joins.
    //! In particular,
    //! - SELECT * FROM AAA JOIN BBB JOIN CCC is actually (SELECT * FROM AAA JOIN BBB) JOIN CCC.
    //! Thus, OperandIndex is always 0 or 1.
    int OperandIndex = 0;
    bool Dynamic = false;
    bool IsPartitioned = false;

    //! Only for dynamic tables.
    NTabletClient::TTableMountInfoPtr TableMountInfo;

    TTable(NYPath::TRichYPath path, const NYTree::IAttributeDictionaryPtr& attributes);

    bool IsSortedDynamic() const;
};

TString ToString(const TTablePtr& table);

////////////////////////////////////////////////////////////////////////////////

// Fetches tables for given paths.
// If `skipUnsuitableNodes` is set, skips all non-static-table items,
// otherwise throws an error for them.
std::vector<TTablePtr> FetchTables(
    TQueryContext* queryContext,
    const std::vector<NYPath::TRichYPath>& richPaths,
    bool skipUnsuitableNodes,
    bool enableDynamicStoreRead,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
