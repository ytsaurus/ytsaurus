#pragma once

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/controller_agent/persistence.h>

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

private:
    struct TInputReadRange
    {
        NTableClient::TKeyBound LowerBound;
        NTableClient::TKeyBound UpperBound;

        PHOENIX_DECLARE_TYPE(TInputReadRange, 0xb40a7395);
    };
    std::vector<TInputReadRange> Ranges_;

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_TYPE(TReadRangeRegistry, 0x343abac7);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
