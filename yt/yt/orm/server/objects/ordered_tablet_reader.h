#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EOffsetType, ui8,
    ((Exact)         (0))
    ((Earliest)      (1))
    ((LastCommitted) (2))
);

class TOrderedTabletReader
    : public TRefCounted
{
public:
    TOrderedTabletReader(
        TTimestamp barrierTimestamp,
        const TDBTable* table,
        i64 tabletIndex,
        NYT::NApi::TTabletInfo tabletInfo,
        ISession* session,
        std::optional<NYPath::TYPath> consumerPath,
        bool skipTrimmed);

    void SetEarliestOffset();
    void SetLastCommittedOffset();
    void SetOffset(i64 offset, TTimestamp timestamp);
    i64 GetOffset() const;
    int GetTabletIndex() const;
    TTimestamp GetTimestamp() const;

    bool HasMoreRows() const;

    // Accumulates skipped row count with respect to previous searched offset.
    // Does not include trimmed rows for timestamp search as
    // it is impossible to guess which rows had this timestamp before trim.
    i64 GetSkippedRows() const;

    void ScheduleSearchOffset(TTimestamp timestamp);

    //! Output is sorted iff row count limit is specified.
    void ScheduleRead(
        std::function<void(NQueueClient::IQueueRowsetPtr)> handler,
        std::optional<i64> rowCountLimit);

private:
    const TTimestamp BarrierTimestamp_;
    const TDBTable* const Table_;
    const int TabletIndex_;
    const NYT::NApi::TTabletInfo TabletInfo_;
    ISession* const Session_;
    const std::optional<NYPath::TYPath> ConsumerPath_;
    const bool SkipTrimmed_;
    const NLogging::TLogger Logger;

    EOffsetType OffsetType_ = EOffsetType::Exact;
    i64 RowOffset_ = 0;
    TTimestamp Timestamp_ = NTransactionClient::NullTimestamp;
    bool HasMoreRows_ = true;
    i64 SkippedRows_ = 0;

    void ScheduleSearchOffsetViaPullQueue(TTimestamp timestamp);

    void ScheduleValidateRowExistence(
        i64 rowIndex,
        TTimestamp searchedTimestamp);

    void ScheduleSearchOffsetImpl(
        i64 lowerRowIndex,
        i64 upperRowIndex,
        TTimestamp timestamp,
        std::optional<TTimestamp> prevMiddleTimestamp);
};

DEFINE_REFCOUNTED_TYPE(TOrderedTabletReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
