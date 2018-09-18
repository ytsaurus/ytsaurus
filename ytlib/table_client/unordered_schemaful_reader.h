#pragma once

#include "public.h"

namespace NYT {
namespace NTableClient {

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

} // namespace NTableClient
} // namespace NYT
