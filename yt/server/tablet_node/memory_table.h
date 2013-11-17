#pragma once

#include "public.h"
#include "row.h"

#include <core/misc/rcu_tree.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TMemoryTable
    : public TRefCounted
{
public:
    explicit TMemoryTable(
        TTabletManagerConfigPtr config,
        TTablet* tablet);

    void WriteRows(
        TTransaction* transaction,
        NVersionedTableClient::IReaderPtr reader,
        bool transient,
        std::vector<TRowGroup>* lockedGroups);

    static void ConfirmPrewrittenGroup(TRowGroup group);
    static void CommitGroup(TRowGroup group);
    static void AbortGroup(TRowGroup group);

private:
    class TComparer;

    TTabletManagerConfigPtr Config_;
    TTablet* Tablet_;

    TChunkedMemoryPool TreePool_;
    TChunkedMemoryPool RowPool_;
    TChunkedMemoryPool StringPool_;

    std::unique_ptr<TComparer> Comparer_;
    std::unique_ptr<TRcuTree<TRowGroup, TComparer>> Tree_;


    TRowGroup WriteRow(
        TTransaction* transaction,
        NVersionedTableClient::TRow row,
        bool prewrite);

    void InternValue(
        NVersionedTableClient::TRowValue* dst,
        const NVersionedTableClient::TRowValue& src);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
