#pragma once

#include "persistence.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TTabletReader
    : public TRefCounted
{
public:
    TTabletReader(
        TTimestamp timestamp,
        const TDBTable* table,
        i64 tabletIndex,
        const NYT::NApi::TTabletInfo& tabletInfo,
        ISession* session);

    void SetOffset(i64 offset);
    void ScheduleSearchOffset(TTimestamp timestamp);
    i64 GetOffset() const;
    bool HasMoreRows() const;

    void ScheduleRead(
        const std::vector<const TDBField*>& fields,
        std::function<void(const NYT::NApi::IUnversionedRowsetPtr&)> handler,
        std::optional<i64> rowCountLimit);

private:
    void ScheduleSearchOffsetImpl(
        i64 lowerRowIndex,
        i64 upperRowIndex,
        TTimestamp timestamp);
    void ScheduleCheckRowExistence(
        i64 rowIndex);
    void ThrowRowsAlreadyTrimmedException() const;

    const TTimestamp Timestamp_;
    const TDBTable* Table_;
    const i64 TabletIndex_;
    const NYT::NApi::TTabletInfo TabletInfo_;
    ISession* Session_;
    i64 RowOffset_ = 0;
    bool HasMoreRows_ = true;
};

DEFINE_REFCOUNTED_TYPE(TTabletReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
