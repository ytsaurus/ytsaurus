#include "meta.h"

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/formats/arrow/arrow_filter.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <contrib/ydb/core/tx/columnshard/engines/scheme/index_info.h>

#include <contrib/ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

NKikimrTxColumnShard::TIndexPortionMeta TPortionMeta::SerializeToProto() const {
    FullValidation();
    NKikimrTxColumnShard::TIndexPortionMeta portionMeta;
    portionMeta.SetTierName(TierName);
    portionMeta.SetCompactionLevel(CompactionLevel);
    portionMeta.SetDeletionsCount(DeletionsCount);
    portionMeta.SetRecordsCount(RecordsCount);
    portionMeta.SetColumnRawBytes(ColumnRawBytes);
    portionMeta.SetColumnBlobBytes(ColumnBlobBytes);
    portionMeta.SetIndexRawBytes(IndexRawBytes);
    portionMeta.SetIndexBlobBytes(IndexBlobBytes);
    switch (Produced) {
        case TPortionMeta::EProduced::UNSPECIFIED:
            Y_ABORT_UNLESS(false);
        case TPortionMeta::EProduced::INSERTED:
            portionMeta.SetIsInserted(true);
            break;
        case TPortionMeta::EProduced::COMPACTED:
            portionMeta.SetIsCompacted(true);
            break;
        case TPortionMeta::EProduced::SPLIT_COMPACTED:
            portionMeta.SetIsSplitCompacted(true);
            break;
        case TPortionMeta::EProduced::EVICTED:
            portionMeta.SetIsEvicted(true);
            break;
        case TPortionMeta::EProduced::INACTIVE:
            Y_ABORT("Unexpected inactive case");
            //portionMeta->SetInactive(true);
            break;
    }

    portionMeta.MutablePrimaryKeyBordersV1()->SetFirst(FirstPKRow.GetData());
    portionMeta.MutablePrimaryKeyBordersV1()->SetLast(LastPKRow.GetData());
    if (!HasAppData() || AppDataVerified().ColumnShardConfig.GetPortionMetaV0Usage()) {
        portionMeta.SetPrimaryKeyBorders(
            NArrow::TFirstLastSpecialKeys(FirstPKRow.GetData(), LastPKRow.GetData(), PKSchema).SerializePayloadToString());
    }

    RecordSnapshotMin.SerializeToProto(*portionMeta.MutableRecordSnapshotMin());
    RecordSnapshotMax.SerializeToProto(*portionMeta.MutableRecordSnapshotMax());
    for (auto&& i : GetBlobIds()) {
        *portionMeta.AddBlobIds() = i.GetLogoBlobId().AsBinaryString();
    }
    return portionMeta;
}

TString TPortionMeta::DebugString() const {
    TStringBuilder sb;
    sb << "(produced=" << Produced << ";";
    if (TierName) {
        sb << "tier_name=" << TierName << ";";
    }
    sb << ")";
    return sb;
}

std::optional<TString> TPortionMeta::GetTierNameOptional() const {
    if (TierName && TierName != NBlobOperations::TGlobal::DefaultStorageId) {
        return TierName;
    } else {
        return std::nullopt;
    }
}

TString TPortionAddress::DebugString() const {
    return TStringBuilder() << "(path_id=" << PathId << ";portion_id=" << PortionId << ")";
}

}   // namespace NKikimr::NOlap
