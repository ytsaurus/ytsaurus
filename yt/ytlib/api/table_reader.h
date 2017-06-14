#include "client.h"
#include "native_client.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/ypath/public.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

TFuture<NTableClient::ISchemalessMultiChunkReaderPtr> CreateTableReader(
    INativeClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options);

NConcurrency::IAsyncZeroCopyInputStreamPtr CreateBlobTableReader(
    NTableClient::ISchemalessChunkReaderPtr reader,
    const TNullable<TString>& partIndexColumnName,
    const TNullable<TString>& dataColumnName,
    i64 startPartIndex = 0);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
