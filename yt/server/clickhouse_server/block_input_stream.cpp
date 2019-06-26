#include "block_input_stream.h"

#include "type_translation.h"
#include "helpers.h"
#include "db_helpers.h"

#include <yt/client/table_client/schemaless_reader.h>
#include <yt/client/table_client/name_table.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TBlockInputStream
    : public DB::IBlockInputStream
{
public:
    TBlockInputStream(ISchemalessReaderPtr reader, TTableSchema readSchema, TLogger logger)
        : Reader_(std::move(reader))
        , ReadSchema_(std::move(readSchema))
        , Logger(std::move(logger))
    {
        PrepareHeader();
    }

    virtual std::string getName() const override
    {
        return "BlockInputStream";
    }

    virtual DB::Block getHeader() const override
    {
        return HeaderBlock_;
    }

private:
    ISchemalessReaderPtr Reader_;
    TTableSchema ReadSchema_;
    TLogger Logger;
    DB::Block HeaderBlock_;
    std::vector<int> IdToColumnIndex_;

    DB::Block readImpl() override
    {
        auto block = HeaderBlock_.cloneEmpty();

        // TODO(max42): consult with psushin@ about contract here.
        std::vector<TUnversionedRow> rows;
        // TODO(max42): make customizable.
        constexpr int rowsPerRead = 10 * 1024;
        rows.reserve(rowsPerRead);
        while (true) {
            if (!Reader_->Read(&rows)) {
                return {};
            } else if (rows.empty()) {
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
            } else {
                break;
            }
        }

        for (const auto& row : rows) {
            for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
                const auto& value = row[index];
                auto id = value.Id;
                int columnIndex = (id < IdToColumnIndex_.size()) ? IdToColumnIndex_[id] : -1;
                Y_VERIFY(columnIndex != -1);
                switch (value.Type) {
                    case EValueType::Null:
                        // TODO(max42): consider transforming to Y_ASSERT.
                        Y_VERIFY(!ReadSchema_.Columns()[columnIndex].Required());
                        block.getByPosition(columnIndex).column->assumeMutable()->insertDefault();
                        break;

                    // NB(max42): When rewriting this properly, remember that Int64 may
                    // correspond to shorter integer columns.
                    case EValueType::String:
                    case EValueType::Any:
                    case EValueType::Int64:
                    case EValueType::Uint64:
                    case EValueType::Double:
                    case EValueType::Boolean: {
                        auto field = ConvertToField(value);
                        block.getByPosition(index).column->assumeMutable()->insert(field);
                        break;
                    }
                    default:
                        Y_UNREACHABLE();
                }
            }
        }

        return block;
    }

    void PrepareHeader()
    {
        const auto& dataTypes = DB::DataTypeFactory::instance();

        for (int index = 0; index < static_cast<int>(ReadSchema_.Columns().size()); ++index) {
            const auto& columnSchema = ReadSchema_.Columns()[index];
            auto type = RepresentYtType(columnSchema.GetPhysicalType());
            auto dataType = dataTypes.get(GetTypeName(type));
            auto column = dataType->createColumn();
            if (!columnSchema.Required()) {
                column = DB::ColumnNullable::create(std::move(column), DB::ColumnVector<UInt8>::create());
                dataType = DB::makeNullable(dataType);
            }
            HeaderBlock_.insert({ std::move(column), dataType, columnSchema.Name() });
            auto id = Reader_->GetNameTable()->GetIdOrRegisterName(columnSchema.Name());
            if (static_cast<int>(IdToColumnIndex_.size()) <= id) {
                IdToColumnIndex_.resize(id + 1, -1);
            }
            IdToColumnIndex_[id] = index;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreateBlockInputStream(
    ISchemalessReaderPtr reader,
    TTableSchema readSchema,
    TLogger logger)
{
    return std::make_shared<TBlockInputStream>(std::move(reader), std::move(readSchema), logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
