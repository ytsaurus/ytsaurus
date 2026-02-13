#pragma once

#include "public.h"

#include <yt/yt/library/formats/format.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

IFormattedTableReaderPtr CreateEncodedRowStream(
    IRowBatchReaderPtr batchReader,
    NTableClient::TNameTablePtr nameTable,
    NFormats::TFormat format,
    NTableClient::TTableSchemaPtr tableSchema,
    std::optional<std::vector<std::string>> columns,
    NFormats::TControlAttributesConfigPtr controlAttributesConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
