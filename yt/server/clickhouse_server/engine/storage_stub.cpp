#include "storage_stub.h"

#include <yt/server/clickhouse_server/native/table_schema.h>

namespace DB {

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

}   // namespace DB

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageStub
    : public IStorage
{
private:
    const NNative::TTablePtr Table;
    const NamesAndTypesList Columns;

public:
    TStorageStub(NNative::TTablePtr table)
        : Table(std::move(table))
    {}

    std::string getName() const override { return "YT"; }

    std::string getTableName() const override { return Table->Name; }

    bool isRemote() const override { return true; }

    BlockInputStreams read(
        const Names& columnNames,
        const SelectQueryInfo& queryInfo,
        const Context& context,
        QueryProcessingStage::Enum& processedStage,
        size_t maxBlockSize,
        unsigned numStreams) override
    {
        throw Exception(
            "TStorageStub: read() is not supported",
            ErrorCodes::NOT_IMPLEMENTED);
    }
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr CreateStorageStub(NNative::TTablePtr table)
{
    return std::make_shared<TStorageStub>(std::move(table));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
