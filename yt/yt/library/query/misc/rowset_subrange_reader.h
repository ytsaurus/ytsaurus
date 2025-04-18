#pragma once

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/unversioned_reader.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateRowsetSubrangeReader(
    TFuture<TSharedRange<TUnversionedRow>> asyncRows,
    std::pair<TKeyBoundRef, TKeyBoundRef> readRange);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
