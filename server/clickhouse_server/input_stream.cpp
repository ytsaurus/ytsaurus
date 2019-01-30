#include "input_stream.h"

#include "column_builder.h"
#include "db_helpers.h"

#include <yt/server/clickhouse_server/table_reader.h>

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>

#include <sstream>
#include <string>
#include <vector>

namespace NYT::NClickHouseServer {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TStorageInputStream
    : public IProfilingBlockInputStream
{
private:
    const ITableReaderPtr TableReader;

    const Block Sample;

public:
    TStorageInputStream(ITableReaderPtr tableReader)
        : TableReader(std::move(tableReader))
        , Sample(PrepareBlock())
    {}

    std::string getName() const override;

    Block getHeader() const override { return Sample; }

private:
    Block readImpl() override;

    Block PrepareBlock() const;
    TColumnBuilderList PrepareColumns(const Block& block) const;
};

////////////////////////////////////////////////////////////////////////////////

std::string TStorageInputStream::getName() const
{
    const std::vector<TTablePtr>& tables = TableReader->GetTables();
    if (tables.size() == 1) {
        return tables.front()->Name;
    } else {
        // TODO
        return "TableConcatenation";
    }
}

Block TStorageInputStream::readImpl()
{
    auto block = Sample.cloneEmpty();

    auto columns = PrepareColumns(block);
    if (TableReader->Read(columns)) {
        return block;
    }

    return {};
}

Block TStorageInputStream::PrepareBlock() const
{
    const auto& dataTypes = DataTypeFactory::instance();

    Block block;
    for (const auto& column : TableReader->GetColumns()) {
        auto dataType = dataTypes.get(GetTypeName(column));
        block.insert({ std::move(dataType), column.Name });
    }

    return block;
}

TColumnBuilderList TStorageInputStream::PrepareColumns(const Block& block) const
{
    TColumnBuilderList columnBuilders;
    columnBuilders.resize(block.columns());

    const auto& readerColumns = TableReader->GetColumns();

    for (size_t i = 0; i < block.columns(); ++i) {
        auto column = block.getByPosition(i);
        columnBuilders[i] = CreateColumnBuilder(readerColumns[i].Type, column.column->assumeMutable());
    }

    return columnBuilders;
}

////////////////////////////////////////////////////////////////////////////////

BlockInputStreamPtr CreateStorageInputStream(ITableReaderPtr tableReader)
{
    return std::make_shared<TStorageInputStream>(
        std::move(tableReader));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
