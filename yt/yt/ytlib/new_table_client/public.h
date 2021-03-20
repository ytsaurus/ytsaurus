#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

// For read_span_refiner.h
using NTableClient::TLegacyKey;
using NTableClient::TRowRange;

// For rowset_builder.h
using NTableClient::TRowBuffer;
using NTableClient::TMutableVersionedRow;

// For segment_readers.h
using NTableClient::EValueType;
using NTableClient::TUnversionedValue;
using NTableClient::TVersionedValue;
using NTableClient::TUnversionedRow;
using NTableClient::TTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

