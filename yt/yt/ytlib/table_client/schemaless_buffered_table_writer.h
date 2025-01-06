#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/ypath/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemalessBufferedTableWriter(
    TBufferedTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    NApi::NNative::IClientPtr client,
    TNameTablePtr nameTable,
    const NYPath::TYPath& path,
    NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
