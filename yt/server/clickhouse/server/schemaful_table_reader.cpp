#include "schemaful_table_reader.h"

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaful_reader_adapter.h>

#include <yt/client/table_client/name_table.h>
#include <yt/ytlib/api/native/table_reader.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NConcurrency;
using namespace NYT::NApi;
using namespace NYT::NApi::NNative;
using namespace NYT::NTableClient;
using namespace NYT::NYPath;

////////////////////////////////////////////////////////////////////////////////

ISchemafulReaderPtr CreateSchemafulTableReader(
    const NNative::IClientPtr& client,
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

} // namespace NClickHouse
} // namespace NYT
