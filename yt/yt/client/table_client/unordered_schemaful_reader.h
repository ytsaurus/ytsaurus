#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateUnorderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader,
    int concurrency);

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateOrderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader);

ISchemafulReaderPtr CreatePrefetchingOrderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader);

ISchemafulReaderPtr CreateFullPrefetchingOrderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
