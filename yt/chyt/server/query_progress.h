#pragma once

#include "private.h"

#include <IO/Progress.h>

#include <atomic>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

struct TProgressValues
{
    bool Finished = false;

    i64 ReadRows = 0;
    i64 ReadBytes = 0;
    i64 TotalRowsToRead = 0;
    i64 TotalBytesToRead = 0;
};

void ToProto(NProto::TProgressValues* protoProgress, const TProgressValues& progress);

struct TQueryProgressValues
{
    TProgressValues TotalProgress;
    THashMap<TGuid, TProgressValues> SecondaryProgress;
};

void ToProto(NProto::TQueryProgressValues* protoProgress, const TQueryProgressValues& progress);

////////////////////////////////////////////////////////////////////////////////

class TQueryProgress
{
public:
    //! Methods to record overall progress of the query.
    //! They are concurrently called via callback
    //! from different parts of the query pipeline execution.
    void Add(const DB::Progress& progress);
    void Finish();

    //! Methods to record progress of secondary queries
    //! from other instances of the clique involved in execution.
    //!
    //! NB: are not thread-safe, because they are called from RemoteSource,
    //! which is processed synchronously.
    void AddSecondaryProgress(TGuid queryId, const DB::ReadProgress& progress);
    void FinishSecondaryQuery(TGuid queryId);

    TQueryProgressValues GetValues() const;

private:
    TProgressValues LoadTotalValues() const;

    std::atomic<bool> Finished_ = false;

    std::atomic<i64> ReadRows_ = 0;
    std::atomic<i64> ReadBytes_ = 0;
    std::atomic<i64> TotalRowsToRead_ = 0;
    std::atomic<i64> TotalBytesToRead_ = 0;

    THashMap<TGuid, TProgressValues> SecondaryProgress_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
