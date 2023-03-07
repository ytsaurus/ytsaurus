#include "log_reader.h"

#include "bootstrap.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/transaction.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>

#include <yt/core/misc/fs.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <util/generic/buffer.h>
#include <util/string/split.h>
#include <util/system/fstat.h>

namespace NYT::NLogTailer {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

bool TryParseInstantFromLogInstant(TString logInstant, TInstant& instant)
{
    //          01234567890123456789012
    // Log:     2019-07-12 13:43:05,183
    // Iso8601: 2019-07-12T13:43:05.183+0300
    if (static_cast<int>(logInstant.size()) != 23) {
        return false;
    }
    logInstant[10] = 'T';
    logInstant[19] = '.';
    logInstant += "+0300";
    return TInstant::TryParseIso8601(logInstant, instant);
}

TString GetBoundaryTimestampString(const TLogRecord& firstRecord, const TLogRecord& lastRecord)
{
    // It may happen that boundary timestamps are ill-formed as there may appear arbitrary garbage in logs.
    bool boundaryTimestampsWellFormed = true;
    TInstant minTimestamp;
    boundaryTimestampsWellFormed &= TryParseInstantFromLogInstant(firstRecord.Timestamp, minTimestamp);
    TInstant maxTimestamp;
    boundaryTimestampsWellFormed &= TryParseInstantFromLogInstant(lastRecord.Timestamp, maxTimestamp);
    if (boundaryTimestampsWellFormed) {
        return Format("{Min: %v, Max: %v, Lag: %v}", minTimestamp, maxTimestamp, GetInstant() - minTimestamp);
    } else {
        return "(n/a)";
    }
}

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
    record.Size = rawLogRecord.size();

    return record;
}

TUnversionedRow LogRecordToUnversionedRow(
    const TLogRecord& record,
    const TRowBufferPtr& rowBuffer,
    const TNameTablePtr& nameTable,
    const std::vector<std::pair<TString, TString>>& extraLogTableColumns = {})
{
    TUnversionedRowBuilder builder;
    builder.AddValue(ToUnversionedValue(record.Timestamp, rowBuffer, nameTable->GetId("timestamp")));
    builder.AddValue(ToUnversionedValue(record.Increment, rowBuffer, nameTable->GetId("increment")));
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
    , RowBuffer_(New<TRowBuffer>())
    , LogTableNameTable_(New<TNameTable>())
    , Logger("LogTailer")
    , ExtraLogTableColumns_(std::move(extraLogTableColumns))
    , Profiler_("/log_tailer", {TProfileManager::Get()->RegisterTag("filename", Config_->Path)})
{
    Logger.AddTag("LogFile: %v", Config_->Path);

    std::vector<TYPath> paths;
    for (const auto& tableConfig : Config_->Tables) {
        const auto& path = tableConfig->Path;
        if (!WaitFor(Bootstrap_->GetMasterClient()->NodeExists(path)).ValueOrThrow()) {
            YT_LOG_FATAL("Log table does not exist; exiting (TablePath: %v)", path);
        }
        paths.emplace_back(path);
    }
    Logger.AddTag("TablePaths: %v", paths);

    try {
        DoOpenLogFile();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Cannot open log file");
        Log_ = std::nullopt;
    }

    LogTableNameTable_->RegisterName("timestamp");
    LogTableNameTable_->RegisterName("increment");
    LogTableNameTable_->RegisterName("category");
    LogTableNameTable_->RegisterName("message");
    LogTableNameTable_->RegisterName("log_level");
    LogTableNameTable_->RegisterName("thread_id");
    LogTableNameTable_->RegisterName("fiber_id");
    LogTableNameTable_->RegisterName("trace_id");

    for (const auto& [key, value] : ExtraLogTableColumns_) {
        LogTableNameTable_->RegisterName(key);
    }
}

void TLogFileReader::ReadLog()
{
    if (TInstant::Now() - LastLogReadTime_ < Bootstrap_->GetConfig()->ReadPeriod) {
        return;
    }

    LastLogReadTime_ = TInstant::Now();

    DoReadLog();
}

void TLogFileReader::OnLogRotation()
{
    DoReadLog();

    Log_ = std::nullopt;
    try {
        DoOpenLogFile();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Cannot reopen log file");
        Log_ = std::nullopt;
    }
}

void TLogFileReader::OnTermination()
{
    DoReadLog();
}

void TLogFileReader::OnProfiling()
{
    Profiler_.Enqueue("/rows_written", TotalRowsWritten_, EMetricType::Counter);
    Profiler_.Enqueue("/bytes_written", TotalBytesWritten_, EMetricType::Counter);
    Profiler_.Enqueue("/unparsed_rows", TotalUnparsedRows_, EMetricType::Counter);
    Profiler_.Enqueue("/write_errors", TotalWriteErrors_, EMetricType::Counter);
    Profiler_.Enqueue("/trimmed_rows", TotalTrimmedRows_, EMetricType::Counter);
    Profiler_.Enqueue("/trimmed_bytes", TotalTrimmedBytes_, EMetricType::Counter);
    Profiler_.Enqueue("/buffer_size", RecordsBuffer_.size(), EMetricType::Gauge);
    {
        ui64 writeLag = 0;
        if (EarliestRecordTimestamp_) {
            writeLag = (TInstant::Now() - *EarliestRecordTimestamp_).MilliSeconds();
        }
        Profiler_.Enqueue("/write_lag", writeLag, EMetricType::Gauge);
    }
}

i64 TLogFileReader::GetTotalBytesRead() const
{
    return TotalBytesRead_;
}

void TLogFileReader::DoReadLog()
{
    YT_LOG_INFO("Reading started");
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
    OnProfiling();

    YT_LOG_INFO("Reading finished");
}

void TLogFileReader::DoOpenLogFile()
{
    if (!Log_) {
        TFile file(Config_->Path, OpenExisting | RdOnly | Seq);
        TFileStat fstat(file.GetHandle());
        YT_LOG_INFO("Log is not open; trying to open (Inode: %v)", fstat.INode);
        Log_ = TUnbufferedFileInput(file);
        FileOffset_ = 0;
        YT_LOG_INFO("File opened");
    }
}

void TLogFileReader::DoReadBuffer()
{
    auto bufferSize = Bootstrap_->GetConfig()->ReadBufferSize;
    TBuffer buffer(bufferSize);
    while (true) {
        YT_LOG_INFO("Reading from log file");
        TWallTimer timer;
        int bytesRead = Log_->Read(buffer.data(), bufferSize);
        YT_LOG_INFO("Read from log file (ByteCount: %v, ElapsedTime: %v)", bytesRead, timer.GetElapsedTime());
        if (bytesRead == 0) {
            break;
        }
        for (int index = 0; index < bytesRead; ++index) {
            char symbol = buffer.data()[index];
            if (symbol == '\n') {
                auto currentIncrement = NextIncrement_++;
                if (Buffer_) {
                    TLogRecord record;
                    try {
                        record = ParseLogRecord(Buffer_);
                    } catch (const std::exception& ex) {
                        YT_LOG_DEBUG(ex, "Cannot parse log record (Offset: %v, RecordPrefix: %Qv)",
                            FileOffset_ + index - Buffer_.size(),
                            Buffer_.substr(20));
                        ++TotalUnparsedRows_;
                        Buffer_.clear();
                        continue;
                    }
                    record.Increment = currentIncrement;
                    RecordsBuffer_.push_back(record);
                    Buffer_.clear();
                }
            } else {
                Buffer_ += symbol;
            }
        }
        FileOffset_ += bytesRead;
        TotalBytesRead_ += bytesRead;
    }

    EarliestRecordTimestamp_ = std::nullopt;
    for (const auto& logRecord : RecordsBuffer_) {
        TInstant timestamp;
        if (TryParseInstantFromLogInstant(logRecord.Timestamp, timestamp)) {
            EarliestRecordTimestamp_ = timestamp;
            break;
        }
    }
}

bool TLogFileReader::TryProcessRecordRange(TIteratorRange<TLogRecordBuffer::iterator> recordRange)
{
    auto rowsToWrite = recordRange.size();
    YT_ASSERT(rowsToWrite > 0);

    auto boundaryTimestamps = GetBoundaryTimestampString(*recordRange.begin(), *(recordRange.end() - 1));

    YT_LOG_INFO("Processing rows (FirstRecordIncrement: %v, LastRecordIncrement: %v, RecordCount: %v, BoundaryTimestamps: %v)",
        recordRange.begin()->Increment,
        (recordRange.end() - 1)->Increment,
        recordRange.size(),
        boundaryTimestamps);

    TTransactionStartOptions transactionStartOptions;
    transactionStartOptions.Atomicity = NTransactionClient::EAtomicity::None;
    auto transactionOrError = WaitFor(
        Bootstrap_->GetMasterClient()->StartTransaction(
            NTransactionClient::ETransactionType::Tablet,
            transactionStartOptions));

    if (!transactionOrError.IsOK()) {
        YT_LOG_WARNING(transactionOrError, "Error starting transaction");
        return false;
    }

    const auto& transaction = transactionOrError.Value();

    TWallTimer timer;

    std::vector<std::vector<TUnversionedRow>> rowsPerTable;

    rowsPerTable.resize(Config_->Tables.size());

    ui64 bytesToWrite = 0;
    for (size_t index = 0; index < rowsToWrite; ++index) {
        const auto& record = recordRange[index];
        bytesToWrite += record.Size;
        for (size_t tableIndex = 0; tableIndex < Config_->Tables.size(); ++tableIndex) {
            if (Config_->Tables[tableIndex]->RequireTraceId && record.TraceId.empty()) {
                continue;
            }
            rowsPerTable[tableIndex].emplace_back(LogRecordToUnversionedRow(
                record,
                RowBuffer_,
                LogTableNameTable_,
                ExtraLogTableColumns_));
        }
    }

    for (size_t tableIndex = 0; tableIndex < Config_->Tables.size(); ++tableIndex) {
        const auto& tableConfig = Config_->Tables[tableIndex];
        const auto& rows = rowsPerTable[tableIndex];
        YT_LOG_DEBUG("Writing rows to table (TableIndex: %v, Path: %v, RowCount: %v, TransactionId: %v)",
            tableIndex,
            tableConfig->Path,
            rows.size(),
            transaction->GetId());
        transaction->WriteRows(
            tableConfig->Path,
            LogTableNameTable_,
            TSharedRange<NTableClient::TUnversionedRow>{rows, MakeStrong(this)});
    }

    auto commitResultOrError = WaitFor(transaction->Commit());
    RowBuffer_->Clear();

    if (commitResultOrError.IsOK()) {
        YT_LOG_INFO("Rows committed (RecordCount: %v, TransactionId: %v, ElapsedTime: %v, BoundaryTimestamps: %v)",
            recordRange.size(),
            transaction->GetId(),
            timer.GetElapsedTime(),
            boundaryTimestamps);
        TotalRowsWritten_ += rowsToWrite;
        TotalBytesWritten_ += bytesToWrite;
        return true;
    } else {
        YT_LOG_WARNING(commitResultOrError, "Error committing rows (RecordCount: %v, TransactionId: %v, ElapsedTime: %v, BoundaryTimestamps: %v)",
            recordRange.size(),
            transaction->GetId(),
            timer.GetElapsedTime(),
            boundaryTimestamps);
        ++TotalWriteErrors_;
        return false;
    }
}

void TLogFileReader::DoWriteRows()
{
    int recordsBufferPtr = 0;
    while (recordsBufferPtr < RecordsBuffer_.size()) {
        i64 rowsToWrite = std::min<i64>(RecordsBuffer_.size() - recordsBufferPtr, Bootstrap_->GetConfig()->MaxRecordsPerTransaction);
        YT_ASSERT(rowsToWrite > 0);

        auto success = TryProcessRecordRange(
            MakeIteratorRange(RecordsBuffer_.begin() + recordsBufferPtr, RecordsBuffer_.begin() + recordsBufferPtr + rowsToWrite));
        if (!success) {
            break;
        }

        recordsBufferPtr += rowsToWrite;
    }

    int recordsLeftInBuffer = RecordsBuffer_.size() - recordsBufferPtr;

    i64 maxRecordsInBuffer = Bootstrap_->GetConfig()->MaxRecordsInBuffer;
    if (recordsLeftInBuffer > maxRecordsInBuffer) {
        YT_LOG_WARNING("Too many records in buffer; trimming (RecordCount: %v, MaxRecordCount: %v, TrimmedBoundaryTimestamps: %v)",
            recordsLeftInBuffer,
            maxRecordsInBuffer,
            GetBoundaryTimestampString(*(RecordsBuffer_.end() - recordsLeftInBuffer), *(RecordsBuffer_.end() - maxRecordsInBuffer - 1)));

        int rowsToTrim = recordsLeftInBuffer - maxRecordsInBuffer;
        TotalTrimmedRows_ += rowsToTrim;
        for (int index = recordsBufferPtr; index < recordsBufferPtr + rowsToTrim; ++index) {
            TotalTrimmedBytes_ += RecordsBuffer_[index].Size;
        }
        recordsLeftInBuffer = maxRecordsInBuffer;
    }

    RecordsBuffer_.erase(RecordsBuffer_.begin(), RecordsBuffer_.end() - recordsLeftInBuffer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
