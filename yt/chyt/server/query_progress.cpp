#include "query_progress.h"

#include <yt/chyt/client/protos/query_service.pb.h>

#include <yt/yt/core/misc/guid.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TProgressValues* protoProgress, const TProgressValues& progress)
{
    protoProgress->set_read_rows(progress.ReadRows);
    protoProgress->set_read_bytes(progress.ReadBytes);
    protoProgress->set_total_rows_to_read(progress.TotalRowsToRead);
    protoProgress->set_total_bytes_to_read(progress.TotalBytesToRead);

    protoProgress->set_finished(progress.Finished);
}

void ToProto(NProto::TQueryProgressValues* protoProgress, const TQueryProgressValues& progress)
{
    ToProto(protoProgress->mutable_total_progress(), progress.TotalProgress);

    protoProgress->mutable_secondary_query_ids()->Reserve(progress.SecondaryProgress.size());
    protoProgress->mutable_secondary_query_progresses()->Reserve(progress.SecondaryProgress.size());
    for (auto& [queryId, secondaryProgress] : progress.SecondaryProgress) {
        ToProto(protoProgress->mutable_secondary_query_ids()->Add(), queryId);
        ToProto(protoProgress->mutable_secondary_query_progresses()->Add(), secondaryProgress);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TQueryProgress::Add(const DB::Progress& progress)
{
    ReadRows_ += progress.read_rows;
    ReadBytes_ += progress.read_bytes;
    TotalRowsToRead_ += progress.total_rows_to_read;
    TotalBytesToRead_ += progress.total_bytes_to_read;
}

void TQueryProgress::AddSecondaryProgress(TGuid queryId, const DB::ReadProgress& progress)
{
    // It's expected that the object has already been created.
    auto& progressHolder = SecondaryProgress_[queryId];

    progressHolder.ReadRows += progress.read_rows;
    progressHolder.ReadBytes += progress.read_bytes;
    progressHolder.TotalRowsToRead += progress.total_rows_to_read;
    progressHolder.TotalBytesToRead += progress.total_bytes_to_read;
}

void TQueryProgress::FinishSecondaryQuery(TGuid queryId)
{
    SecondaryProgress_[queryId].Finished = true;
}

void TQueryProgress::Finish()
{
    Finished_.store(true);
}

TQueryProgressValues TQueryProgress::GetValues() const
{
    TQueryProgressValues values;

    values.TotalProgress = LoadTotalValues();
    for (auto& [queryId, progress] : SecondaryProgress_) {
        values.SecondaryProgress[queryId] = progress;
    }

    return values;
};

TProgressValues TQueryProgress::LoadTotalValues() const
{
    TProgressValues values;

    values.Finished = Finished_.load(std::memory_order_relaxed);

    values.ReadRows = ReadRows_.load(std::memory_order_relaxed);
    values.ReadBytes = ReadBytes_.load(std::memory_order_relaxed);
    values.TotalRowsToRead = TotalRowsToRead_.load(std::memory_order_relaxed);
    values.TotalBytesToRead = TotalBytesToRead_.load(std::memory_order_relaxed);

    return values;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
