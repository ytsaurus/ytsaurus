#include "chunk_index_builder.h"

#include <yt/yt/client/table_client/versioned_row.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkIndexBuilder
    : public IChunkIndexBuilder
{
public:
    i64 GetSectorAlignmentSize() const override
    {
        YT_UNIMPLEMENTED();
    }

    bool IsGroupReorderingEnabled() const override
    {
        YT_UNIMPLEMENTED();
    }

    int GetGroupCount() const override
    {
        YT_UNIMPLEMENTED();
    }

    int GetGroupIdFromValueId(int /*valueId*/) const override
    {
        YT_UNIMPLEMENTED();
    }

    void ProcessRow(
        TVersionedRow /*row*/,
        int /*blockIndex*/,
        i64 /*rowOffset*/,
        i64 /*rowLength*/,
        TRange<int> /*groupOffsets*/,
        TRange<int> /*groupIndexes*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TSharedRef BuildIndex() override
    {
        YT_UNIMPLEMENTED();
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkIndexBuilderPtr CreateChunkIndexBuilder()
{
    return New<TChunkIndexBuilder>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
