#pragma once

#include "public.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateUnorderedSchemafulReader(
    std::function<ISchemafulReaderPtr()> getNextReader,
    int concurrency);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
