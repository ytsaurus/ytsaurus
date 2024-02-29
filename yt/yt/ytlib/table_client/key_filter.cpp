#include "key_filter.h"
#include "private.h"

#include <yt/yt/ytlib/table_client/config.h>

#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/library/xor_filter/xor_filter.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTableClient {

using namespace NProto;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

const static auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

bool Contains(const TXorFilter& filter, TLegacyKey key, int keyPrefixLength)
{
    return filter.Contains(GetFarmFingerprint(key.FirstNElements(keyPrefixLength)));
}

////////////////////////////////////////////////////////////////////////////////

class TXorFilterBuilder
{
public:
    TXorFilterBuilder(
        TKeyFilterWriterConfigPtr config,
        int keyPrefixLength)
        : Config_(std::move(config))
        , KeyPrefixLength_(keyPrefixLength)
    { }

    void AddKey(TVersionedRow row)
    {
        auto fingerprint = GetFarmFingerprint(row.Keys().Slice(0, KeyPrefixLength_));
        // NB: We eliminate fingerprint repetitions over consecutive similar prefixes
        // but we do not care much about unlikely random fingerprint matches.
        if (Keys_.empty() || Keys_.back() != fingerprint) {
            Keys_.push_back(fingerprint);
        }
    }

    void FlushBlock(TLegacyKey lastKey, bool force)
    {
        if (Keys_.empty()) {
            return;
        }

        if (force ||
            TXorFilter::ComputeByteSize(ssize(Keys_), Config_->EffectiveBitsPerKey) > Config_->BlockSize)
        {
            DoFlushBlock(lastKey);
        }
    }

    std::vector<TSharedRef> SerializeBlocks(NProto::TSystemBlockMetaExt* systemBlockMetaExt)
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

private:
    const TKeyFilterWriterConfigPtr Config_;
    const int KeyPrefixLength_;

    std::vector<TFingerprint> Keys_;

    std::vector<TSharedRef> ProducedBlocks_;
    std::vector<NProto::TSystemBlockMeta> ProducedMetas_;

    bool FilterBuildingFailed_ = false;


    void DoFlushBlock(TLegacyKey lastKey)
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
        ToProto(xorFilterMetaExt->mutable_last_key(), lastKey.Elements());
        xorFilterMetaExt->set_key_prefix_length(KeyPrefixLength_);

        ProducedBlocks_.push_back(std::move(filterData));
        ProducedMetas_.push_back(std::move(protoMeta));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TMultiXorFilterBuilder
    : public IKeyFilterBuilder
{
public:
    TMultiXorFilterBuilder(
        const TChunkWriterConfigPtr& config,
        int keyColumnCount)
    {
        if (config->KeyFilter->Enable) {
            Builders_.emplace_back(
                config->KeyFilter,
                keyColumnCount);
        }

        if (config->KeyPrefixFilter->Enable) {
            for (auto length : config->KeyPrefixFilter->PrefixLengths) {
                auto keyPrefixLength = std::min(length, keyColumnCount);
                YT_VERIFY(keyPrefixLength > 0);
                // NB: When reading meta filters are distinguished only by their length,
                // so we need to assert that we do not build key filters of same length.
                if (keyPrefixLength < keyColumnCount || !config->KeyFilter->Enable) {
                    Builders_.emplace_back(
                        config->KeyPrefixFilter,
                        keyPrefixLength);
                }
            }
        }
    }

    void AddKey(TVersionedRow row) override
    {
        for (auto& builder : Builders_) {
            builder.AddKey(row);
        }
    }

    std::vector<TSharedRef> SerializeBlocks(NProto::TSystemBlockMetaExt* systemBlockMetaExt) override
    {
        std::vector<TSharedRef> resultBlocks;
        for (auto& builder : Builders_) {
            auto blocks = builder.SerializeBlocks(systemBlockMetaExt);
            resultBlocks.insert(
                resultBlocks.end(),
                std::make_move_iterator(blocks.begin()),
                std::make_move_iterator(blocks.end()));
        }

        return resultBlocks;
    }

    void FlushBlock(TLegacyKey lastKey, bool force) override
    {
        for (auto& builder : Builders_) {
            builder.FlushBlock(lastKey, force);
        }
    }

    NChunkClient::EBlockType GetBlockType() const override
    {
        return NChunkClient::EBlockType::XorFilter;
    }

private:
    std::vector<TXorFilterBuilder> Builders_;
};

////////////////////////////////////////////////////////////////////////////////

IKeyFilterBuilderPtr CreateXorFilterBuilder(
    const TChunkWriterConfigPtr& config,
    int keyColumnCount)
{
    if (!config->KeyFilter->Enable &&
        !config->KeyPrefixFilter->Enable)
    {
        return nullptr;
    }

    return New<TMultiXorFilterBuilder>(config, keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
