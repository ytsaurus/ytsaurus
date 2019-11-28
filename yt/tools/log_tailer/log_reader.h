#pragma once

#include "config.h"
#include "public.h"

#include <yt/client/table_client/schema.h>

#include <yt/core/concurrency/periodic_executor.h>

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
};

////////////////////////////////////////////////////////////////////////////////

class TLogFileReader
    : public TIntrinsicRefCounted
{
public:
    TLogFileReader(
        TLogFileConfigPtr config,
        TBootstrap* bootstrap,
        std::vector<std::pair<TString, TString>> extraLogTableColumns);

    void Start();

    void Stop();

    void OnLogRotation();

private:
    void DoReadLog();
    void DoOpenLogFile();
    void DoReadBuffer();
    void DoWriteRows();

    TLogFileConfigPtr Config_;
    TBootstrap* const Bootstrap_;

    NConcurrency::TPeriodicExecutorPtr LogReaderExecutor_;

    std::optional<TUnbufferedFileInput> Log_;

    NQueryClient::TRowBufferPtr RowBuffer_;

    TString Buffer_;

    NTableClient::TNameTablePtr LogTableNameTable_;

    NLogging::TLogger Logger;

    std::deque<TLogRecord> RecordsBuffer_;

    i64 FileOffset_ = 0;

    std::vector<std::pair<TString, TString>> ExtraLogTableColumns_;
};

DEFINE_REFCOUNTED_TYPE(TLogFileReader)

////////////////////////////////////////////////////////////////////////////////

}
