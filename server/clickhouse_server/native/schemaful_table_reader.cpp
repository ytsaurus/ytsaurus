#include "schemaful_table_reader.h"

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/api/native/table_reader.h>

#include <yt/client/table_client/schemaful_reader_adapter.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

using namespace NConcurrency;
using namespace NApi;
using namespace NApi::NNative;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////    ////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulTableReader(
    const NApi::NNative::IClientPtr& client,
    const TRichYPath& path,
    const TTableSchema& schema,
    const NApi::TTableReaderOptions& options,
    const TColumnFilter& columnFilter)
{
    auto schemalessReaderFactory = [=] (
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) -> ISchemalessReaderPtr
    {
        return WaitFor(
            CreateSchemalessMultiChunkReader(
                client,
                path,
                options,
                nameTable,
                columnFilter))
            .ValueOrThrow();
    };

    return CreateSchemafulReaderAdapter(
        std::move(schemalessReaderFactory),
        schema,
        columnFilter);
}

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
