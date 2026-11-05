#include "logging_transform.h"

namespace NYT::NClickHouseServer {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TLoggingTransform::TLoggingTransform(const DB::Block& header, const TLogger& logger)
    : DB::ISimpleTransform(header, header, false)
    , Logger(logger)
{ }

std::string TLoggingTransform::getName() const
{
    return "LoggingTransform";
}

void TLoggingTransform::transform(DB::Chunk& chunk)
{
    auto now = TInstant::Now();

    auto elapsed = now - LastLogTime;

    if (LastLogTime != TInstant::Zero() && (LastLogTime + TDuration::Seconds(1) < now)) {
        YT_TLOG_DEBUG("Reading took significant time")
            .With("WallTime", elapsed);
    }

    YT_TLOG_TRACE("Chunk read")
        .With("Elapsed", elapsed)
        .With("RowCount", chunk.getNumRows())
        .With("ColumnCount", chunk.getNumColumns());

    LastLogTime = now;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
