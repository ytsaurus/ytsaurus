#include "block_output_stream.h"

#include "helpers.h"
#include "conversion.h"
#include "config.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/client/api/transaction.h>
#include <yt/client/table_client/unversioned_writer.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/row_buffer.h>

#include <yt/core/concurrency/scheduler.h>

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeFactory.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TBlockOutputStreamBase
    : public DB::IBlockOutputStream
{
public:
    TBlockOutputStreamBase(TTableSchemaPtr schema, const TLogger& logger)
        : NameTable_(New<TNameTable>())
        , Logger(logger)
        , RowBuffer_(New<TRowBuffer>())
        , Schema_(std::move(schema))
    {
        HeaderBlock_ = ToHeaderBlock(*Schema_, New<TCompositeSettings>());

        for (const auto& column : Schema_->Columns()) {
            PositionToId_.emplace_back(NameTable_->GetIdOrRegisterName(column.Name()));
        }
    }

    virtual DB::Block getHeader() const override
    {
        return HeaderBlock_;
    }

    virtual void write(const DB::Block& block) override
    {
        YT_LOG_TRACE("Writing block (RowCount: %v, ColumnCount: %v, ByteCount: %v)", block.rows(), block.columns(), block.bytes());

        int rowCount = block.rows();
        int columnCount = block.columns();

        std::vector<DB::ColumnPtr> columns(columnCount);
        std::vector<DB::ColumnPtr> nestedColumns(columnCount);
        for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
            auto column = block.getByPosition(columnIndex).column;
            if (auto* nullableColumn = DB::checkAndGetColumn<DB::ColumnNullable>(column.get())) {
                nestedColumns[columnIndex] = nullableColumn->getNestedColumnPtr();
            } else {
                nestedColumns[columnIndex] = column;
            }
            columns[columnIndex] = std::move(column);
        }

        std::vector<TUnversionedRow> rows;
        rows.reserve(rowCount);

        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            auto row = RowBuffer_->AllocateUnversioned(columnCount);

            for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
                const EValueType columnType = Schema_->Columns()[columnIndex].GetPhysicalType();
                auto& value = row[columnIndex];
                value.Id = PositionToId_[columnIndex];
                value.Aggregate = false;

                if (columns[columnIndex]->isNullAt(rowIndex)) {
                    if (Schema_->Columns()[columnIndex].Required()) {
                        THROW_ERROR_EXCEPTION("Value NULL is not allowed in required column %Qv",
                            Schema_->Columns()[columnIndex].Name());
                    }
                    value.Type = EValueType::Null;
                } else {
                    const auto& column = nestedColumns[columnIndex];
                    value.Type = columnType;
                    switch (columnType) {
                        case EValueType::Int64: {
                            value.Data.Int64 = column->getInt(rowIndex);
                            break;
                        }
                        case EValueType::Uint64: {
                            value.Data.Uint64 = column->getUInt(rowIndex);
                            break;
                        }
                        case EValueType::Double: {
                            value.Data.Double = column->getFloat64(rowIndex);
                            break;
                        }
                        case EValueType::Boolean: {
                            ui64 boolValue = column->getUInt(rowIndex);
                            if (boolValue > 1) {
                                THROW_ERROR_EXCEPTION("Cannot convert value %v to boolean", boolValue);
                            }
                            value.Data.Boolean = boolValue;
                            break;
                        }
                        case EValueType::String: {
                            StringRef stringValue = column->getDataAt(rowIndex);
                            value.Data.String = stringValue.data;
                            value.Length = stringValue.size;
                            break;
                        }
                        default: {
                            THROW_ERROR_EXCEPTION("Unexpected data type %Qlv", value.Type);
                        }
                    }
                }
            }
            rows.push_back(row);
        }

        struct TRowsHolder
            : public TRefCounted
        {
            TRowBufferPtr RowBuffer;
            std::vector<DB::ColumnPtr> Columns;
        };
        auto holder = New<TRowsHolder>();
        holder->RowBuffer = RowBuffer_;
        holder->Columns = std::move(columns);

        DoWriteRows(std::move(rows), holder);

        RowBuffer_->Clear();
    }

protected:
    TNameTablePtr NameTable_;
    TLogger Logger;

    using THolderPtr = TIntrusivePtr<TRefCounted>;
    // Holder contains smart pointers to data referred by rows. Rows can be accessed safely as long as holder is alive.
    virtual void DoWriteRows(std::vector<TUnversionedRow> rows, THolderPtr holder) = 0;

private:
    TRowBufferPtr RowBuffer_;
    TTableSchemaPtr Schema_;
    std::vector<int> PositionToId_;
    DB::Block HeaderBlock_;
};

////////////////////////////////////////////////////////////////////////////////

class TStaticTableBlockOutputStream
    : public TBlockOutputStreamBase
{
public:
    TStaticTableBlockOutputStream(
        TRichYPath path,
        TTableSchemaPtr schema,
        TTableWriterConfigPtr config,
        NNative::IClientPtr client,
        const TLogger& logger)
        : TBlockOutputStreamBase(std::move(schema), logger)
    {
        Writer_ = WaitFor(CreateSchemalessTableWriter(
            std::move(config),
            New<NTableClient::TTableWriterOptions>(),
            std::move(path),
            NameTable_,
            std::move(client),
            nullptr /* transaction */))
            .ValueOrThrow();
    }

    virtual void writeSuffix() override
    {
        YT_LOG_INFO("Closing writer");
        WaitFor(Writer_->Close())
            .ThrowOnError();
        YT_LOG_INFO("Writer closed");
    }

private:
    IUnversionedWriterPtr Writer_;

    virtual void DoWriteRows(std::vector<TUnversionedRow> rows, THolderPtr holder)
    {
        if (!Writer_->Write(rows)) {
            WaitFor(Writer_->GetReadyEvent())
                .ThrowOnError();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableBlockOutputStream
    : public TBlockOutputStreamBase
{
public:
    TDynamicTableBlockOutputStream(
        TRichYPath path,
        TTableSchemaPtr schema,
        TDynamicTableSettingsPtr settings,
        NNative::IClientPtr client,
        const TLogger& logger)
        : TBlockOutputStreamBase(std::move(schema), logger)
        , Path_(std::move(path))
        , Settings_(std::move(settings))
        , Client_(std::move(client))
    { }

private:
    TRichYPath Path_;
    TDynamicTableSettingsPtr Settings_;
    NNative::IClientPtr Client_;

    virtual void DoWriteRows(std::vector<TUnversionedRow> rows, THolderPtr holder) override
    {
        i64 rowCount = rows.size();
        int rowsPerWrite = Settings_->MaxRowsPerWrite;
        int writeCount = (rowCount + rowsPerWrite - 1) / rowsPerWrite;

        auto range = MakeSharedRange<TUnversionedRow>(std::move(rows), std::move(holder));

        for (int writeIndex = 0; writeIndex < writeCount; ++writeIndex) {
            int from = writeIndex * rowCount / writeCount;
            int to = (writeIndex + 1) * rowCount / writeCount;

            bool isWritten = false;
            std::vector<TError> errors;

            for (int retryIndex = 0; retryIndex <= Settings_->WriteRetryCount; ++retryIndex) {
                if (retryIndex != 0) {
                    NConcurrency::TDelayedExecutor::WaitForDuration(Settings_->WriteRetryBackoff);
                }

                auto error = TryWriteRowRange(range.Slice(from, to));
                if (error.IsOK()) {
                    isWritten = true;
                    break;
                }

                YT_LOG_DEBUG(error, "Error writing row range, backing off (RetryIndex: %v, LowerRowIndex: %v, UpperRowIndex: %v)",
                    retryIndex,
                    from,
                    to);

                errors.emplace_back(std::move(error));
            }

            if (!isWritten) {
                THROW_ERROR_EXCEPTION("Cannot write rows to dynamic table %Qv: all retries failed", Path_.GetPath())
                    << errors;
            }
        }
    }

    TError TryWriteRowRange(TSharedRange<TUnversionedRow> rows)
    {
        TTransactionStartOptions transactionStartOptions {
            .Atomicity = Settings_->TransactionAtomicity,
        };
        auto transactionOrError = WaitFor(
            Client_->StartTransaction(
                ETransactionType::Tablet,
                transactionStartOptions));

        if (!transactionOrError.IsOK()) {
            YT_LOG_WARNING(transactionOrError, "Error starting transaction");
            return transactionOrError;
        }
        const auto& transaction = transactionOrError.Value();

        YT_LOG_DEBUG("Writing rows to table (Path: %v, RowCount: %v, TransactionId: %v)",
            Path_.GetPath(),
            rows.size(),
            transaction->GetId());

        transaction->WriteRows(
            Path_.GetPath(),
            NameTable_,
            rows);

        auto commitResultOrError = WaitFor(transaction->Commit());

        if (commitResultOrError.IsOK()) {
            YT_LOG_DEBUG("Rows committed (RowCount: %v, TransactionId: %v)",
                rows.size(),
                transaction->GetId());
        } else {
            YT_LOG_INFO(commitResultOrError, "Error committing rows (RowCount: %v, TransactionId: %v)",
                rows.size(),
                transaction->GetId());
        }

        return commitResultOrError;
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::BlockOutputStreamPtr CreateStaticTableBlockOutputStream(
    TRichYPath path,
    TTableSchemaPtr schema,
    TTableWriterConfigPtr config,
    NNative::IClientPtr client,
    const TLogger& logger)
{
    return std::make_shared<TStaticTableBlockOutputStream>(
        std::move(path),
        std::move(schema),
        std::move(config),
        std::move(client),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

DB::BlockOutputStreamPtr CreateDynamicTableBlockOutputStream(
    TRichYPath path,
    TTableSchemaPtr schema,
    TDynamicTableSettingsPtr settings,
    NNative::IClientPtr client,
    const TLogger& logger)
{
    return std::make_shared<TDynamicTableBlockOutputStream>(
        std::move(path),
        std::move(schema),
        std::move(settings),
        std::move(client),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
