#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr MaybeWrapWithTranslatingAdapter(
    ISchemafulUnversionedReaderPtr underlyingReader,
    const TTableSchemaPtr& tableSchema,
    const std::vector<TColumnIdMapping>& schemaIdMapping);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
