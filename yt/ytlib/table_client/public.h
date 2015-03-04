#pragma once

#include <core/misc/public.h>
#include <core/misc/small_vector.h>

#include <ytlib/table_client/public.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControlAttribute,
    (TableIndex)
);

////////////////////////////////////////////////////////////////////////////////

const int DefaultPartitionTag = -1;
const i64 MaxRowWeightLimit = (i64) 128 * 1024 * 1024;
const size_t MaxColumnNameSize = 256;
const int MaxColumnCount = 1024;
const size_t MaxKeySize = (i64) 4 * 1024;
const int FormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

class TTableChunkReader;
typedef TIntrusivePtr<TTableChunkReader> TTableChunkReaderPtr;

class TTableChunkReaderProvider;
typedef TIntrusivePtr<TTableChunkReaderProvider> TTableChunkReaderProviderPtr;

class TChannelReader;
typedef TIntrusivePtr<TChannelReader> TChannelReaderPtr;

class TChunkReaderOptions;
typedef TIntrusivePtr<TChunkReaderOptions> TChunkReaderOptionsPtr;

typedef SmallVector< std::pair<TStringBuf, TStringBuf>, 32 > TRow;
typedef std::vector<Stroka> TKeyColumns;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
