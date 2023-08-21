#pragma once

#include "config.h"
#include "public.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <util/generic/iterator_range.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

struct TLogRecord
{
    TString Timestamp;
    TString Message;
    TString JobId;
    TString Category;
    TString LogLevel;
    TString ThreadId;
    TString FiberId;
    TString TraceId;
    ui64 Increment;
    ui64 Size;
};

////////////////////////////////////////////////////////////////////////////////

class TLogFileReader
    : public TRefCounted
{
public:
    TLogFileReader(
        TLogFileConfigPtr config,
        TBootstrap* bootstrap,
        std::vector<std::pair<TString, TString>> extraLogTableColumns);

    void ReadLog();

    void OnLogRotation();

    void OnTermination();

    i64 GetTotalBytesRead() const;

private:
    TLogFileConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    std::optional<TUnbufferedFileInput> Log_;

    NQueryClient::TRowBufferPtr RowBuffer_;

    TString Buffer_;

    NTableClient::TNameTablePtr LogTableNameTable_;

    NLogging::TLogger Logger;

    using TLogRecordBuffer = std::deque<TLogRecord>;
    TLogRecordBuffer RecordsBuffer_;

    i64 FileOffset_ = 0;
    i64 TotalBytesRead_ = 0;

    std::vector<std::pair<TString, TString>> ExtraLogTableColumns_;

    ui64 NextIncrement_ = 0;

    TInstant LastLogReadTime_;

    NProfiling::TCounter TotalBytesWritten_;
    NProfiling::TCounter TotalRowsWritten_;
    NProfiling::TCounter TotalWriteErrors_;
    NProfiling::TCounter TotalUnparsedRows_;
    NProfiling::TCounter TotalTrimmedRows_;
    NProfiling::TCounter TotalTrimmedBytes_;
    NProfiling::TGauge RecordBufferSize_;
    NProfiling::TEventTimer WriteLag_;

    void DoReadLog();
    void DoOpenLogFile();
    void DoReadBuffer();
    void DoWriteRows();
    bool TryProcessRecordRange(TIteratorRange<TLogRecordBuffer::iterator> recordRange);
};

DEFINE_REFCOUNTED_TYPE(TLogFileReader)

////////////////////////////////////////////////////////////////////////////////

}
