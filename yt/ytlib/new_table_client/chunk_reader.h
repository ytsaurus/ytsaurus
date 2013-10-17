#pragma once

#include "public.h"
#include "reader.h"

#include <core/misc/ref.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

IReaderPtr CreateChunkReader(
    const TReadLimit& startLimit,
    const TReadLimit& endLimit,
    TTimestamp timestamp = NullTimestamp);


class TChunkReader
    : public IReader
{
public:
    TChunkReader(
        const TReadLimit& startLimit,
        const TReadLimit& endLimit,
        const TNullable<TTimestamp>& timestamp);

    virtual TAsyncError Open(
        TNameTablePtr nameTable, 
        const TSchema& schema, 
        bool includeAllColumns,
        ERowetType type = ERowsetType::Simple) override;

    virtual bool Read(std::vector<TRow>* rows) override;
    virtual TAsyncError GetReadyEvent() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
