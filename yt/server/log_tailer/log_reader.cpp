#include "log_reader.h"

#include "bootstrap.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/transaction.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>

#include <util/generic/buffer.h>
#include <util/string/split.h>

namespace NYT::NLogTailer {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TLogRecord ParseLogRecord(const TString& rawLogRecord)
{
    TVector<TString> tokens;
    StringSplitter(rawLogRecord).Split('\t').Collect(&tokens);
    if (tokens.size() != 7) {
        THROW_ERROR_EXCEPTION("Expected 7 tokens in log record, got %v", tokens.size());
    }

    TLogRecord record;
    record.Timestamp = tokens[0];
    record.LogLevel = tokens[1];
    record.Category = tokens[2];
    record.Message = tokens[3];
    record.ThreadId = tokens[4];
    record.FiberId = tokens[5];
    record.TraceId = tokens[6];

    return record;
}

TUnversionedRow LogRecordToUnversionedRow(
    const TLogRecord& record,
    ui64 lineIndex,
    const TRowBufferPtr& rowBuffer,
    const TNameTablePtr& nameTable,
    const std::vector<std::pair<TString, TString>>& extraLogTableColumns = {})
{
    TUnversionedRowBuilder builder;
    builder.AddValue(ToUnversionedValue(record.Timestamp, rowBuffer, nameTable->GetId("timestamp")));
    builder.AddValue(ToUnversionedValue(lineIndex, rowBuffer, nameTable->GetId("line_index")));
    builder.AddValue(ToUnversionedValue(record.Category, rowBuffer, nameTable->GetId("category")));
    builder.AddValue(ToUnversionedValue(record.LogLevel, rowBuffer, nameTable->GetId("log_level")));
    builder.AddValue(ToUnversionedValue(record.Message, rowBuffer, nameTable->GetId("message")));
    builder.AddValue(ToUnversionedValue(record.ThreadId, rowBuffer, nameTable->GetId("thread_id")));
    builder.AddValue(ToUnversionedValue(record.FiberId, rowBuffer, nameTable->GetId("fiber_id")));
    builder.AddValue(ToUnversionedValue(record.TraceId, rowBuffer, nameTable->GetId("trace_id")));

    for (const auto& [key, value] : extraLogTableColumns) {
        builder.AddValue(ToUnversionedValue(value, rowBuffer, nameTable->GetId(key)));
    }

    return rowBuffer->Capture(builder.GetRow());
}

////////////////////////////////////////////////////////////////////////////////

TLogFileReader::TLogFileReader(
    TLogFileConfigPtr config,
    TBootstrap* bootstrap,
    std::vector<std::pair<TString, TString>> extraLogTableColumns)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , LogReaderExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetReaderInvoker(),
        BIND(&TLogFileReader::DoReadLog, MakeStrong(this)),
        Bootstrap_->GetConfig()->ReadPeriod))
    , RowBuffer_(New<TRowBuffer>())
    , LogTableNameTable_(New<TNameTable>())
    , Logger("LogReader")
    , ExtraLogTableColumns_(std::move(extraLogTableColumns))
{
    Logger.AddTag("LogFile: %v", Config_->Path);
    Logger.AddTag("TablePaths: %v", Config_->TablePaths);

    try {
        DoOpenLogFile();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Cannot open log file");
        Log_ = std::nullopt;
    }

    LogTableNameTable_->RegisterName("timestamp");
    LogTableNameTable_->RegisterName("line_index");
    LogTableNameTable_->RegisterName("category");
    LogTableNameTable_->RegisterName("message");
    LogTableNameTable_->RegisterName("log_level");
    LogTableNameTable_->RegisterName("thread_id");
    LogTableNameTable_->RegisterName("fiber_id");
    LogTableNameTable_->RegisterName("trace_id");

    for (const auto& [key, value] : ExtraLogTableColumns_) {
        LogTableNameTable_->RegisterName(key);
    }

    for (const auto& table : Config_->TablePaths) {
        if (!WaitFor(Bootstrap_->GetMasterClient()->NodeExists(table)).ValueOrThrow()) {
            YT_LOG_FATAL("Log table does not exist; exiting (TablePath: %v)", table);
        }
    }
}

void TLogFileReader::Start()
{
    LogReaderExecutor_->Start();
}

void TLogFileReader::Stop()
{
    WaitFor(LogReaderExecutor_->Stop())
        .ThrowOnError();
}

void TLogFileReader::OnLogRotation()
{
    YT_LOG_DEBUG("Log is rotating");
    // Read the old log file till the end.
    WaitFor(LogReaderExecutor_->GetExecutedEvent())
        .ThrowOnError();
    // Stop the log reader.
    WaitFor(LogReaderExecutor_->Stop())
        .ThrowOnError();
    // Reopen log.
    Log_ = std::nullopt;
    try {
        DoOpenLogFile();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Cannot reopen log file");
        Log_ = std::nullopt;
    }
    // Start log reader over the new log file.
    Start();
}

void TLogFileReader::OnTermination()
{
    WaitFor(LogReaderExecutor_->Stop())
        .ThrowOnError();
    DoReadLog();
}

void TLogFileReader::DoReadLog()
{
    try {
        DoOpenLogFile();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Cannot open log file");
        return;
    }

    try {
        DoReadBuffer();
        DoWriteRows();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Unexpected error");
    }
}

void TLogFileReader::DoOpenLogFile()
{
    if (!Log_) {
        YT_LOG_DEBUG("Log is not open; trying to open");
        Log_ = TUnbufferedFileInput(Config_->Path);
    }
}

void TLogFileReader::DoReadBuffer()
{
    auto bufferSize = Bootstrap_->GetConfig()->ReadBufferSize;
    TBuffer buffer(bufferSize);
    while (true) {
        int bytesRead = Log_->Read(buffer.data(), bufferSize);
        YT_LOG_DEBUG("Read from log file (ByteCount: %v)", bytesRead);
        if (bytesRead == 0) {
            break;
        }
        for (int index = 0; index < bytesRead; ++index) {
            char symbol = buffer.data()[index];
            if (symbol == '\n') {
                if (Buffer_) {
                    TLogRecord record;
                    try {
                        record = ParseLogRecord(Buffer_);
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(ex, "Cannot parse log record (Offset: %v, RecordPrefix: %Qv)",
                            FileOffset_ + index - Buffer_.size(),
                            Buffer_.substr(20));
                        Buffer_.clear();
                        continue;
                    }
                    RecordsBuffer_.push_back(record);
                    Buffer_.clear();
                }
            } else {
                Buffer_ += symbol;
            }
        }
        FileOffset_ += bytesRead;
    }
}

void TLogFileReader::DoWriteRows()
{
    int recordsBufferPtr = 0;
    while (recordsBufferPtr < RecordsBuffer_.size()) {
        i64 rowsToWrite = std::min<i64>(RecordsBuffer_.size() - recordsBufferPtr, Bootstrap_->GetConfig()->MaxRecordsPerTransaction);

        std::vector<TUnversionedRow> rows;
        rows.reserve(rowsToWrite);
        for (int index = recordsBufferPtr; index < recordsBufferPtr + rowsToWrite; ++index) {
            rows.emplace_back(LogRecordToUnversionedRow(
                RecordsBuffer_[index],
                LineIndex_ + index - recordsBufferPtr,
                RowBuffer_,
                LogTableNameTable_,
                ExtraLogTableColumns_));
        }

        YT_LOG_DEBUG("Writing rows (RowCount: %v)", rows.size());

        bool writingFailed = false;

        for (const auto& table : Config_->TablePaths) {
            YT_LOG_DEBUG("Writing rows to table (Table: %v)", table);

            auto transaction = WaitFor(Bootstrap_->GetMasterClient()->StartTransaction(NTransactionClient::ETransactionType::Tablet))
                .ValueOrThrow();

            transaction->WriteRows(
                table,
                LogTableNameTable_,
                TSharedRange<NTableClient::TUnversionedRow>{rows, MakeStrong(this)});

            try {
                WaitFor(transaction->Commit())
                    .ValueOrThrow();
            } catch (std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to write rows (Table: %v)", table);
                writingFailed = true;
                break;
            }
        }

        if (writingFailed) {
            break;
        }

        recordsBufferPtr += rowsToWrite;
        LineIndex_ += rowsToWrite;
    }

    int recordsLeftInBuffer = RecordsBuffer_.size() - recordsBufferPtr;

    i64 maxRecordsInBuffer = Bootstrap_->GetConfig()->MaxRecordsInBuffer;
    if (recordsLeftInBuffer > maxRecordsInBuffer) {
        YT_LOG_WARNING("Too much records in buffer; trimming (RecordCount: %v, MaxRecordCount: %v)",
            recordsLeftInBuffer,
            maxRecordsInBuffer);
        recordsLeftInBuffer = maxRecordsInBuffer;
    }

    RecordsBuffer_.erase(RecordsBuffer_.begin(), RecordsBuffer_.end() - recordsLeftInBuffer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
