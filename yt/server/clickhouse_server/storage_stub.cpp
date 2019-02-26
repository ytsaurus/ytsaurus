#include "storage_stub.h"

#include "table.h"

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageStub
    : public DB::IStorage
{
private:
    const TClickHouseTablePtr Table;
    const NamesAndTypesList Columns;

public:
    TStorageStub(TClickHouseTablePtr table)
        : Table(std::move(table))
    {}

    std::string getName() const override { return "YT"; }

    std::string getTableName() const override { return Table->Name; }

    bool isRemote() const override { return true; }

    BlockInputStreams read(
        const Names& /* columnNames */,
        const SelectQueryInfo& /* queryInfo */,
        const Context& /* context */,
        QueryProcessingStage::Enum /* processedStage */,
        size_t /* maxBlockSize */,
        unsigned /* numStreams */) override
    {
        throw Exception(
            "TStorageStub: read() is not supported",
            ErrorCodes::NOT_IMPLEMENTED);
    }
};

////////////////////////////////////////////////////////////////////////////////

StoragePtr CreateStorageStub(TClickHouseTablePtr table)
{
    return std::make_shared<TStorageStub>(std::move(table));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
