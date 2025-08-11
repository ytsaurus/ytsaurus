#include "min_hash_digest_builder.h"

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/private.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/min_hash_digest/min_hash_digest.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

using namespace NProto;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TMinHashDigestBuilder
    : public IMinHashDigestBuilder
{
public:
    TMinHashDigestBuilder(TMinHashDigestConfigPtr config)
        : Config_(std::move(config))
    { }

    void OnRow(TVersionedRow row) override
    {
        TFingerprint hash = ComputeHash(row.Keys());

        auto processRow = [hash] <class TComparator> (
            std::map<TFingerprint, TTimestamp>* minHashes,
            TTimestamp timestamp,
            int maxCount,
            TComparator comparator)
        {
            if (ssize(*minHashes) < maxCount) {
                minHashes->emplace(hash, timestamp);
                return;
            }

            if (auto it = minHashes->find(hash); it != minHashes->end()) {
                if (comparator(it->second, timestamp)) {
                    it->second = timestamp;
                }
            } else if (auto lastIt = --minHashes->end(); lastIt->first > hash) {
                minHashes->erase(lastIt);
                minHashes->emplace(hash, timestamp);
            }
        };

        if (!row.WriteTimestamps().Empty()) {
            processRow(&WriteMinHashes_, row.WriteTimestamps().Back(), Config_->WriteCount, std::greater<ui64>());
        }
        if (!row.DeleteTimestamps().empty()) {
            processRow(&DeleteTombstoneMinHashes_, row.DeleteTimestamps().Back(), Config_->DeleteTombstoneCount, std::less<ui64>());
        }
    }

    TSharedRef SerializeBlock(NProto::TSystemBlockMetaExt* systemBlockMetaExt) override
    {
        NProto::TSystemBlockMeta protoMeta;
        protoMeta.MutableExtension(TMinHashDigestSystemBlockMeta::min_hash_digest_block_meta);

        auto* blockMeta = systemBlockMetaExt->add_system_blocks();
        blockMeta->Swap(&protoMeta);

        return TMinHashDigest::Build(WriteMinHashes_, DeleteTombstoneMinHashes_);
    }

    EBlockType GetBlockType() const override
    {
        return EBlockType::MinHashDigest;
    }

private:
    const TMinHashDigestConfigPtr Config_;

    // TODO(dave11ar): Use more efficient data structure.
    std::map<TFingerprint, TTimestamp> WriteMinHashes_;
    std::map<TFingerprint, TTimestamp> DeleteTombstoneMinHashes_;
};

////////////////////////////////////////////////////////////////////////////////

IMinHashDigestBuilderPtr CreateMinHashDigestBuilder(
    TMinHashDigestConfigPtr config)
{
    if (!config) {
        return nullptr;
    }

    return New<TMinHashDigestBuilder>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
