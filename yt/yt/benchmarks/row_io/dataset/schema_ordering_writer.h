#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NRowIOBenchmark {

////////////////////////////////////////////////////////////////////////////////

NTableClient::IUnversionedWriterPtr CreateSchemaOrderingWriter(
    NTableClient::IUnversionedWriterPtr underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowIOBenchmark
