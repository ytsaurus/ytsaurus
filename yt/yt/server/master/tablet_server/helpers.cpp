#include "helpers.h"

#include "config.h"

#include <yt/yt/server/master/table_server/table_node.h>

namespace NYT::NTabletServer {

using namespace NTableServer;
using namespace NCypressClient;
using namespace NChunkServer;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

bool IsHunkChunk(const TTabletBase* tablet, const TChunkTree* child)
{
    if (!child) {
        return false;
    }

    if (tablet->GetType() == EObjectType::Tablet && IsJournalChunkType(child->GetType())) {
        return true;
    }

    if (!IsBlobChunkType(child->GetType())) {
        return false;
    }

    const auto* chunk = child->AsChunk();
    return chunk->GetChunkType() == EChunkType::Hunk;
}

bool IsDynamicStoreReadEnabled(
    const TTableNode* table,
    const TDynamicTabletManagerConfigPtr& dynamicConfig)
{
    if (table->IsPhysicallyLog() && !table->IsReplicated()) {
        return false;
    }

    if (table->GetActualTabletState() == ETabletState::Unmounted) {
        return table->GetEnableDynamicStoreRead().value_or(
            dynamicConfig->EnableDynamicStoreReadByDefault);
    } else {
        return table->GetMountedWithEnabledDynamicStoreRead();
    }
}

static TError TryParseTabletRange(const TTabletOwnerBase* table, int* first, int* last)
{
    const auto& tablets = table->Tablets();
    if (*first == -1 && *last == -1) {
        *first = 0;
        *last = ssize(tablets) - 1;
    } else {
        if (*first < 0 || *first >= std::ssize(tablets)) {
            return TError("First tablet index %v is out of range [%v, %v]",
                *first,
                0,
                tablets.size() - 1);
        }
        if (*last < 0 || *last >= std::ssize(tablets)) {
            return TError("Last tablet index %v is out of range [%v, %v]",
                *last,
                0,
                tablets.size() - 1);
        }
        if (*first > *last) {
            return TError("First tablet index is greater than last tablet index");
        }
    }

    return TError();
}

void ParseTabletRange(TTabletOwnerBase* owner, int* first, int* last)
{
    auto error = TryParseTabletRange(owner, first, last);
    YT_VERIFY(error.IsOK());
}

void ParseTabletRangeOrThrow(const TTabletOwnerBase* table, int* first, int* last)
{
    TryParseTabletRange(table, first, last)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
