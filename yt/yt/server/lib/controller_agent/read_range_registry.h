#pragma once

#include "persistence.h"

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! This class is responsible for holding input read ranges for
//! data slices.
//! TODO(max42): YT-13880.
class TReadRangeRegistry
{
public:
    TReadRangeRegistry() = default;

    void RegisterDataSlice(const NChunkClient::TLegacyDataSlicePtr& dataSlice);

    void ApplyReadRange(
        const NChunkClient::TLegacyDataSlicePtr& dataSlice,
        const NTableClient::TComparator& comparator) const;

    void Persist(const TPersistenceContext& context);

private:
    struct TInputReadRange
    {
        NTableClient::TKeyBound LowerBound;
        NTableClient::TKeyBound UpperBound;

        void Persist(const TPersistenceContext& context);
    };
    std::vector<TInputReadRange> Ranges_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
