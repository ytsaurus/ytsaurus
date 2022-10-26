#include "chunk_index_builder.h"

#include <yt/yt/client/table_client/versioned_row.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkIndexBuilder
    : public IChunkIndexBuilder
{
public:
    void ProcessRow(
        TVersionedRow /*row*/,
        int /*blockIndex*/,
        i64 /*rowOffset*/,
        i64 /*rowLength*/,
        TRange<int> /*groupOffsets*/,
        TRange<int> /*groupIndexes*/) override
    {
        return;
    }

    TSharedRef BuildIndex() override
    {
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkIndexBuilderPtr CreateChunkIndexBuilder()
{
    return New<TChunkIndexBuilder>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
