#include "write_set.h"

#include <yt/yt/client/table_client/wire_protocol.h>

namespace NYT::NSequoiaClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TWriteSet* protoWriteSet, const TWriteSet& writeSet)
{
    protoWriteSet->clear_table_to_write_set();

    for (auto table : TEnumTraits<ESequoiaTable>::GetDomainValues()) {
        const auto& tableWriteSet = writeSet[table];
        if (tableWriteSet.empty()) {
            continue;
        }

        NProto::TWriteSet::TTableWriteSet protoTableWriteSet;

        auto wireProtocolWriter = CreateWireProtocolWriter();
        for (const auto& [key, lockedRowInfo] : tableWriteSet) {
            wireProtocolWriter->WriteUnversionedRow(key);
            wireProtocolWriter->WriteLockMask(lockedRowInfo.LockMask);

            ToProto(protoTableWriteSet.add_tablet_ids(), lockedRowInfo.TabletId);
            ToProto(protoTableWriteSet.add_tablet_cell_ids(), lockedRowInfo.TabletCellId);
        }

        auto wireKeys = MergeRefsToString(wireProtocolWriter->Finish());
        protoTableWriteSet.set_keys(wireKeys);

        (*protoWriteSet->mutable_table_to_write_set())[static_cast<int>(table)] = protoTableWriteSet;
    }
}

void FromProto(TWriteSet* writeSet, const NProto::TWriteSet& protoWriteSet, const TRowBufferPtr& rowBuffer)
{
    for (auto table : TEnumTraits<ESequoiaTable>::GetDomainValues()) {
        (*writeSet)[table].clear();
    }

    for (const auto& [tableIndex, protoTableWriteSet] : protoWriteSet.table_to_write_set()) {
        auto table = CheckedEnumCast<ESequoiaTable>(tableIndex);
        auto& tableWriteSet = (*writeSet)[table];

        int rowCount = protoTableWriteSet.tablet_ids_size();

        auto wireKeys = TSharedRef::FromString(protoTableWriteSet.keys());
        auto wireProtocolReader = CreateWireProtocolReader(wireKeys, rowBuffer);
        for (int index = 0; index < rowCount; ++index) {
            auto key = wireProtocolReader->ReadUnversionedRow(/*captureValues*/ true);

            TLockedRowInfo lockedRowInfo;
            lockedRowInfo.LockMask = wireProtocolReader->ReadLockMask();
            FromProto(&lockedRowInfo.TabletId, protoTableWriteSet.tablet_ids(index));
            FromProto(&lockedRowInfo.TabletCellId, protoTableWriteSet.tablet_cell_ids(index));

            EmplaceOrCrash(tableWriteSet, key, lockedRowInfo);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
