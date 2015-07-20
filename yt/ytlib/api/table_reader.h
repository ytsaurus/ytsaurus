#include "client.h"

#include <ytlib/new_table_client/public.h>

#include <ytlib/ypath/rich.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

NVersionedTableClient::ISchemalessMultiChunkReaderPtr CreateTableReader(
    IClientPtr client,
    const NYPath::TRichYPath& path,
    const TTableReaderOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
