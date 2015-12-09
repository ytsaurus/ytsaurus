#include "client.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/ypath/rich.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemalessMultiChunkReaderPtr CreateTableReader(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
