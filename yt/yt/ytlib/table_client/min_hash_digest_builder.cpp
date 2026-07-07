#include "min_hash_digest_builder.h"

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/private.h>

#include <yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/min_hash_digest/min_hash_digest.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NTransactionClient;

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
        , WriteMinHashAccumulator_(Config_->WriteTimestampCount)
        , DeleteMinHashAccumulator_(Config_->DeleteTimestampCount)
    { }

    // We take earliest write and latest delete.
    void OnRow(TVersionedRow row) override
    {
        TFingerprint hash = ComputeHash(row.Keys());

        if (!row.WriteTimestamps().Empty()) {
            WriteMinHashAccumulator_.Add(
                hash,
                UnixTimeFromTimestamp(row.WriteTimestamps().Back()));
        }
        if (!row.DeleteTimestamps().empty()) {
            DeleteMinHashAccumulator_.Add(
                hash,
                UnixTimeFromTimestamp(row.DeleteTimestamps().Front()));
        }
    }

    TSharedRef SerializeBlock(NProto::TSystemBlockMetaExt* systemBlockMetaExt) override
    {
        NProto::TSystemBlockMeta protoMeta;
        protoMeta.MutableExtension(TMinHashDigestSystemBlockMeta::min_hash_digest_block_meta);

        auto* blockMeta = systemBlockMetaExt->add_system_blocks();
        blockMeta->Swap(&protoMeta);

        auto digest = New<TMinHashDigest>(/*memoryTracker*/ nullptr);
        digest->Initialize(WriteMinHashAccumulator_.Finish(), DeleteMinHashAccumulator_.Finish());

        return digest->BuildSerialized();
    }

    EBlockType GetBlockType() const override
    {
        return EBlockType::MinHashDigest;
    }

private:
    const TMinHashDigestConfigPtr Config_;

    TWriteMinHashAccumulator WriteMinHashAccumulator_;
    TDeleteMinHashAccumulator DeleteMinHashAccumulator_;
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
