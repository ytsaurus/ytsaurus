#include "sink_to_storage.h"

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

class TSinkToStorageBase
    : public DB::SinkToStorage
{
public:
    TSinkToStorageBase(
        TTableSchemaPtr schema,
        std::vector<DB::DataTypePtr> dataTypes,
        const TCompositeSettingsPtr& compositeSettings,
        std::function<void()> onFinished,
        const TLogger& logger)
        : DB::SinkToStorage(ToHeaderBlock(*schema, New<TCompositeSettings>()))
        , NameTable_(TNameTable::FromSchema(*schema))
        , Logger(logger)
        , Schema_(std::move(schema))
        , DataTypes_(std::move(dataTypes))
        , ColumnIndexToId_(GetColumnIndexToId(NameTable_, Schema_->GetColumnNames()))
        , CompositeSettings_(std::move(compositeSettings))
        , OnFinished_(std::move(onFinished))
    { }

    void consume(DB::Chunk chunk) override
    {
        YT_LOG_TRACE("Writing block (RowCount: %v, ColumnCount: %v, ByteCount: %v)", chunk.getNumRows(), chunk.getNumColumns(), chunk.bytes());

        // TODO(buyval01): Rewrite ToRowRange to work with chunks to avoid cloning.
        auto rowRange = ToRowRange(getHeader().cloneWithColumns(chunk.detachColumns()), DataTypes_, ColumnIndexToId_, CompositeSettings_);

        DoWriteRows(std::move(rowRange));
    }

    void onFinish() override
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
    std::vector<int> ColumnIndexToId_;
    DB::Block HeaderBlock_;
    TCompositeSettingsPtr CompositeSettings_;
    std::function<void()> OnFinished_;
};

////////////////////////////////////////////////////////////////////////////////

class TSinkToStaticTable
    : public TSinkToStorageBase
{
public:
    TSinkToStaticTable(
        TRichYPath path,
        TTableSchemaPtr schema,
        std::vector<DB::DataTypePtr> dataTypes,
        TTableWriterConfigPtr config,
        TCompositeSettingsPtr compositeSettings,
        NNative::IClientPtr client,
        TTransactionId writeTransactionId,
        std::function<void()> onFinished,
        const TLogger& logger)
        : TSinkToStorageBase(
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

        // TODO(dakovalkov): value statistics and row count are disabled because
        // they are handled inappropriately in 23.1 version.
        // Remove it when all clusters are 23.2+.
        auto options = New<NTableClient::TTableWriterOptions>();
        options->EnableColumnarValueStatistics = false;
        options->EnableRowCountInColumnarStatistics = false;

        Writer_ = WaitFor(CreateSchemalessTableWriter(
            std::move(config),
            std::move(options),
            std::move(path),
            NameTable_,
            std::move(client),
            /*localHostName*/ TString(), // Locality is not important for CHYT.
            std::move(transaction),
            /*writeBlocksOptions*/ {}))
            .ValueOrThrow();
    }

    DB::String getName() const override
    {
        return "SinkToStaticTable";
    }

    void onFinish() override
    {
        YT_LOG_INFO("Closing writer");
        WaitFor(Writer_->Close())
            .ThrowOnError();
        YT_LOG_INFO("Writer closed");

        TSinkToStorageBase::onFinish();
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

class TSinkToDynamicTable
    : public TSinkToStorageBase
{
public:
    TSinkToDynamicTable(
        TRichYPath path,
        TTableSchemaPtr schema,
        std::vector<DB::DataTypePtr> dataTypes,
        TDynamicTableSettingsPtr dynamicTableSettings,
        TCompositeSettingsPtr compositeSettings,
        NNative::IClientPtr client,
        std::function<void()> onFinished,
        const TLogger& logger)
        : TSinkToStorageBase(
            std::move(schema),
            std::move(dataTypes),
            std::move(compositeSettings),
            std::move(onFinished),
            logger)
        , Path_(std::move(path))
        , DynamicTableSettings_(std::move(dynamicTableSettings))
        , Client_(std::move(client))
    { }

    DB::String getName() const override
    {
        return "SinkToDynamicTable";
    }

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

DB::SinkToStoragePtr CreateSinkToStaticTable(
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
    return std::make_shared<TSinkToStaticTable>(
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

DB::SinkToStoragePtr CreateSinkToDynamicTable(
    TRichYPath path,
    TTableSchemaPtr schema,
    std::vector<DB::DataTypePtr> dataTypes,
    TDynamicTableSettingsPtr dynamicTableSettings,
    TCompositeSettingsPtr compositeSettings,
    NNative::IClientPtr client,
    std::function<void()> onFinished,
    const TLogger& logger)
{
    return std::make_shared<TSinkToDynamicTable>(
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
