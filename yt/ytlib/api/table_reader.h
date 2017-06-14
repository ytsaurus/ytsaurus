#include "client.h"

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
    NTableClient::ISchemalessMultiChunkReaderPtr reader,
    const TNullable<TString>& partIndexColumnName,
    const TNullable<TString>& dataColumnName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
