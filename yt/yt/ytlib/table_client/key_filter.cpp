#include "key_filter.h"
#include "private.h"

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/library/xor_filter/xor_filter.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTableClient {

using namespace NProto;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

const static auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

bool Contains(const TXorFilter& filter, TLegacyKey key)
{
    return filter.Contains(GetFarmFingerprint(key.Elements()));
}

////////////////////////////////////////////////////////////////////////////////

class TXorFilterBuilder
    : public IKeyFilterBuilder
{
public:
    explicit TXorFilterBuilder(TKeyFilterWriterConfigPtr config)
        : Config_(std::move(config))
    { }

    void AddKey(TFingerprint fingerprint) override
    {
        Keys_.push_back(fingerprint);
    }

    std::vector<TSharedRef> SerializeBlocks(NProto::TSystemBlockMetaExt* systemBlockMetaExt) override
    {
        YT_VERIFY(Keys_.empty());

        // We could not produce a filter for some block so pretend that the chunk has no filters at all.
        if (FilterBuildingFailed_) {
            return {};
        }

        for (auto& blockMeta : ProducedMetas_) {
            systemBlockMetaExt->add_system_blocks()->Swap(&blockMeta);
        }

        return std::move(ProducedBlocks_);
    }

    void FlushBlock(TLegacyKey key, bool force) override
    {
        if (Keys_.empty()) {
            return;
        }

        if (force || NYT::TXorFilter::ComputeByteSize(ssize(Keys_), Config_->EffectiveBitsPerKey) > Config_->BlockSize) {
            DoFlushBlock(key);
        }
    }

    NChunkClient::EBlockType GetBlockType() const override
    {
        return NChunkClient::EBlockType::XorFilter;
    }

private:
    const TKeyFilterWriterConfigPtr Config_;

    std::vector<TFingerprint> Keys_;

    std::vector<TSharedRef> ProducedBlocks_;
    std::vector<NProto::TSystemBlockMeta> ProducedMetas_;

    bool FilterBuildingFailed_ = false;

    void DoFlushBlock(TLegacyKey key)
    {
        TSharedRef filterData;

        try {
            filterData = NYT::TXorFilter::Build(
                MakeRange(Keys_),
                Config_->EffectiveBitsPerKey,
                Config_->TrialCount);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to build XOR filter (KeyCount: %v)",
                ssize(Keys_));

            Keys_.clear();
            FilterBuildingFailed_ = true;
            return;
        }

        YT_VERIFY(filterData);

        Keys_.clear();

        NProto::TSystemBlockMeta protoMeta;
        auto* xorFilterMetaExt = protoMeta.MutableExtension(TXorFilterSystemBlockMeta::xor_filter_system_block_meta_ext);
        ToProto(xorFilterMetaExt->mutable_last_key(), key.Elements());

        ProducedBlocks_.push_back(std::move(filterData));
        ProducedMetas_.push_back(std::move(protoMeta));
    }
};

////////////////////////////////////////////////////////////////////////////////

IKeyFilterBuilderPtr CreateXorFilterBuilder(TKeyFilterWriterConfigPtr config)
{
    return New<TXorFilterBuilder>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
