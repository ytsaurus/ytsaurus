#pragma once

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/unversioned_reader.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

extern std::pair<TKeyBoundRef, TKeyBoundRef> UniversalRange;

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateRowsetSubrangeReader(
    TFuture<TSharedRange<TUnversionedRow>> asyncRows,
    std::pair<TKeyBoundRef, TKeyBoundRef> readRange = UniversalRange);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
