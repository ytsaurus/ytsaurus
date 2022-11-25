#include "block_output_stream.h"

#include "helpers.h"
#include "conversion.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeFactory.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYPath;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TBlockOutputStreamBase
    : public DB::IBlockOutputStream
{
public:
    TBlockOutputStreamBase(
        TTableSchemaPtr schema,
        std::vector<DB::DataTypePtr> dataTypes,
        const TCompositeSettingsPtr& compositeSettings,
        std::function<void()> onFinished,
        const TLogger& logger)
        : NameTable_(New<TNameTable>())
        , Logger(logger)
        , Schema_(std::move(schema))
        , DataTypes_(std::move(dataTypes))
        , CompositeSettings_(std::move(compositeSettings))
        , OnFinished_(std::move(onFinished))
    {
        HeaderBlock_ = ToHeaderBlock(*Schema_, New<TCompositeSettings>());

        for (const auto& column : Schema_->Columns()) {
            PositionToId_.emplace_back(NameTable_->GetIdOrRegisterName(column.Name()));
        }
    }

    DB::Block getHeader() const override
    {
        return HeaderBlock_;
    }

    void write(const DB::Block& block) override
    {
        YT_LOG_TRACE("Writing block (RowCount: %v, ColumnCount: %v, ByteCount: %v)", block.rows(), block.columns(), block.bytes());

        auto rowRange = ToRowRange(block, DataTypes_, PositionToId_, CompositeSettings_);

        DoWriteRows(std::move(rowRange));
    }

    void writeSuffix() override
    {
        OnFinished_();
    }

protected:
    TNameTablePtr NameTable_;
    TLogger Logger;

    using THolderPtr = TRefCountedPtr;
    // Holder contains smart pointers to data referred by rows. Rows can be accessed safely as long as holder is alive.
    virtual void DoWriteRows(TSharedRange<TUnversionedRow> rows) = 0;

private:
    TTableSchemaPtr Schema_;
    std::vector<DB::DataTypePtr> DataTypes_;
    std::vector<int> PositionToId_;
    DB::Block HeaderBlock_;
    TCompositeSettingsPtr CompositeSettings_;
    std::function<void()> OnFinished_;
};

////////////////////////////////////////////////////////////////////////////////

class TStaticTableBlockOutputStream
    : public TBlockOutputStreamBase
{
public:
    TStaticTableBlockOutputStream(
        TRichYPath path,
        TTableSchemaPtr schema,
        std::vector<DB::DataTypePtr> dataTypes,
        TTableWriterConfigPtr config,
        TCompositeSettingsPtr compositeSettings,
        NNative::IClientPtr client,
        TTransactionId writeTransactionId,
        std::function<void()> onFinished,
        const TLogger& logger)
        : TBlockOutputStreamBase(
            std::move(schema),
            std::move(dataTypes),
            std::move(compositeSettings),
            std::move(onFinished),
            logger)
    {
        NApi::ITransactionPtr transaction;
        if (writeTransactionId) {
            transaction = client->AttachTransaction(writeTransactionId);
        }
        Writer_ = WaitFor(CreateSchemalessTableWriter(
            std::move(config),
            New<NTableClient::TTableWriterOptions>(),
            std::move(path),
            NameTable_,
            std::move(client),
            /*localHostName*/ TString(), // Locality is not important for CHYT.
            std::move(transaction)))
            .ValueOrThrow();
    }

    void writeSuffix() override
    {
        YT_LOG_INFO("Closing writer");
        WaitFor(Writer_->Close())
            .ThrowOnError();
        YT_LOG_INFO("Writer closed");

        TBlockOutputStreamBase::writeSuffix();
    }

private:
    IUnversionedWriterPtr Writer_;

    void DoWriteRows(TSharedRange<TUnversionedRow> rows) override
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
        std::vector<DB::DataTypePtr> dataTypes,
        TDynamicTableSettingsPtr dynamicTableSettings,
        TCompositeSettingsPtr compositeSettings,
        NNative::IClientPtr client,
        std::function<void()> onFinished,
        const TLogger& logger)
        : TBlockOutputStreamBase(
            std::move(schema),
            std::move(dataTypes),
            std::move(compositeSettings),
            std::move(onFinished),
            logger)
        , Path_(std::move(path))
        , DynamicTableSettings_(std::move(dynamicTableSettings))
        , Client_(std::move(client))
    { }

private:
    TRichYPath Path_;
    TDynamicTableSettingsPtr DynamicTableSettings_;
    NNative::IClientPtr Client_;

    void DoWriteRows(TSharedRange<TUnversionedRow> range) override
    {
        i64 rowCount = range.size();
        int rowsPerWrite = DynamicTableSettings_->MaxRowsPerWrite;
        int writeCount = (rowCount + rowsPerWrite - 1) / rowsPerWrite;

        for (int writeIndex = 0; writeIndex < writeCount; ++writeIndex) {
            int from = writeIndex * rowCount / writeCount;
            int to = (writeIndex + 1) * rowCount / writeCount;

            bool isWritten = false;
            std::vector<TError> errors;

            for (int retryIndex = 0; retryIndex <= DynamicTableSettings_->WriteRetryCount; ++retryIndex) {
                if (retryIndex != 0) {
                    NConcurrency::TDelayedExecutor::WaitForDuration(DynamicTableSettings_->WriteRetryBackoff);
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
            .Atomicity = DynamicTableSettings_->TransactionAtomicity,
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
    std::vector<DB::DataTypePtr> dataTypes,
    TTableWriterConfigPtr config,
    TCompositeSettingsPtr compositeSettings,
    NNative::IClientPtr client,
    NTransactionClient::TTransactionId writeTransactionId,
    std::function<void()> onFinished,
    const TLogger& logger)
{
    return std::make_shared<TStaticTableBlockOutputStream>(
        std::move(path),
        std::move(schema),
        std::move(dataTypes),
        std::move(config),
        std::move(compositeSettings),
        std::move(client),
        writeTransactionId,
        std::move(onFinished),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

DB::BlockOutputStreamPtr CreateDynamicTableBlockOutputStream(
    TRichYPath path,
    TTableSchemaPtr schema,
    std::vector<DB::DataTypePtr> dataTypes,
    TDynamicTableSettingsPtr dynamicTableSettings,
    TCompositeSettingsPtr compositeSettings,
    NNative::IClientPtr client,
    std::function<void()> onFinished,
    const TLogger& logger)
{
    return std::make_shared<TDynamicTableBlockOutputStream>(
        std::move(path),
        std::move(schema),
        std::move(dataTypes),
        std::move(dynamicTableSettings),
        std::move(compositeSettings),
        std::move(client),
        std::move(onFinished),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
