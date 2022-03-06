#include "codec.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NErasure {

using namespace ::NErasure;

////////////////////////////////////////////////////////////////////////////////

int ICodec::GetTotalPartCount() const
{
    return GetDataPartCount() + GetParityPartCount();
}

////////////////////////////////////////////////////////////////////////////////

struct TJerasureBlobTag
{ };

struct TLrcBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

struct TCodecTraits
{
    using TBlobType = TSharedRef;
    using TMutableBlobType = TSharedMutableRef;
    using TBufferType = TBlob;
    using ECodecType = ECodec;

    static TMutableBlobType AllocateBlob(size_t size)
    {
        return TMutableBlobType::Allocate<TJerasureBlobTag>(size, false);
    }

    static TBufferType AllocateBuffer(size_t size)
    {
        // Only LRC now uses buffer allocation.
        return TBufferType(TLrcBufferTag(), size);
    }

    static TBlobType FromBufferToBlob(TBufferType&& blob)
    {
        return TBlobType::FromBlob(std::move(blob));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
class TCodec
    : public ICodec
{
public:
    TCodec(ECodec id, bool bytewise)
        : Id_(id)
        , Bytewise_(bytewise)
    { }

    ECodec GetId() const override
    {
        return Id_;
    }

    std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) const override
    {
        return Underlying_.Encode(blocks);
    }

    std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const TPartIndexList& erasedIndices) const override
    {
        return Underlying_.Decode(blocks, erasedIndices);
    }

    bool CanRepair(const TPartIndexList& erasedIndices) const override
    {
        return Underlying_.CanRepair(erasedIndices);
    }

    bool CanRepair(const TPartIndexSet& erasedIndices) const override
    {
        return Underlying_.CanRepair(erasedIndices);
    }

    std::optional<TPartIndexList> GetRepairIndices(const TPartIndexList& erasedIndices) const override
    {
        return Underlying_.GetRepairIndices(erasedIndices);
    }

    int GetDataPartCount() const override
    {
        return Underlying_.GetDataPartCount();
    }

    int GetParityPartCount() const override
    {
        return Underlying_.GetParityPartCount();
    }

    int GetGuaranteedRepairablePartCount() const override
    {
        return Underlying_.GetGuaranteedRepairablePartCount();
    }

    int GetWordSize() const override
    {
        return Underlying_.GetWordSize();
    }

    bool IsBytewise() const override
    {
        return Bytewise_;
    }

private:
    const ECodec Id_;
    const bool Bytewise_;

    TUnderlying Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    // NB: Changing the set of supported codecs or their properties requires master reign promotion.
    switch (id) {
        // These codecs use Jerasure as a backend.
        case ECodec::ReedSolomon_6_3: {
            static TCodec<TCauchyReedSolomonJerasure<6, 3, 8, TCodecTraits>> result(ECodec::ReedSolomon_6_3, /*bytewise*/ false);
            return &result;
        }
        case ECodec::JerasureLrc_12_2_2: {
            static TCodec<TLrcJerasure<12, 4, 8, TCodecTraits>> result(ECodec::JerasureLrc_12_2_2, /*bytewise*/ false);
            return &result;
        }
        // These codecs use ISA-l as a backend.
        case ECodec::ReedSolomon_3_3: {
            static TCodec<TReedSolomonIsa<3, 3, 8, TCodecTraits>> result(ECodec::ReedSolomon_3_3, /*bytewise*/ true);
            return &result;
        }
        case ECodec::IsaReedSolomon_6_3: {
            static TCodec<TReedSolomonIsa<6, 3, 8, TCodecTraits>> result(ECodec::IsaReedSolomon_6_3, /*bytewise*/ true);
            return &result;
        }
        case ECodec::IsaLrc_12_2_2: {
            static TCodec<TLrcIsa<12, 4, 8, TCodecTraits>> result(ECodec::IsaLrc_12_2_2, /*bytewise*/ true);
            return &result;
        }
        default:
            THROW_ERROR_EXCEPTION("Unsupported erasure codec %Qlv", id);
    }
}

const std::vector<ECodec>& GetSupportedCodecIds()
{
    static const std::vector<ECodec> supportedCodecIds = [] {
        const auto& values = TEnumTraits<ECodec>::GetDomainValues();
        return std::vector<ECodec>(values.begin(), values.end());
    }();
    return supportedCodecIds;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

