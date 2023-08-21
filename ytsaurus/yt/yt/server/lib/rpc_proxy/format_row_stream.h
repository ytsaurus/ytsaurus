#pragma once

#include "public.h"

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

IRowStreamEncoderPtr CreateFormatRowStreamEncoder(
    NTableClient::TNameTablePtr nameTable,
    NFormats::TFormat format,
    NTableClient::TTableSchemaPtr tableSchema,
    NFormats::TControlAttributesConfigPtr controlAttributesConfig);

IRowStreamDecoderPtr CreateFormatRowStreamDecoder(
    NTableClient::TNameTablePtr nameTable,
    NFormats::TFormat format,
    NTableClient::TTableSchemaPtr tableSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
