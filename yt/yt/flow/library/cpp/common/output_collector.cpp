#include "output_collector.h"

#include <yt/yt/flow/library/cpp/misc/compact_unversioned_owning_row.h>
#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/core/misc/error.h>

#include <util/digest/city.h>
#include <util/string/hex.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TOutputMessageIdSuffix::TOutputMessageIdSuffix(EMode mode, std::string value)
    : Mode_(mode)
    , Value_(std::move(value))
{ }

TOutputMessageIdSuffix TOutputMessageIdSuffix::FromSequenceNumber()
{
    return TOutputMessageIdSuffix(EMode::SequenceNumber);
}

TOutputMessageIdSuffix TOutputMessageIdSuffix::FromPayloadHash()
{
    return TOutputMessageIdSuffix(EMode::PayloadHash);
}

TOutputMessageIdSuffix TOutputMessageIdSuffix::FromUserDefined(std::string suffix)
{
    THROW_ERROR_EXCEPTION_IF(suffix.empty(), "User-defined output message ID suffix must not be empty");
    return TOutputMessageIdSuffix(EMode::UserDefined, std::move(suffix));
}

TOutputMessageIdSuffix::EMode TOutputMessageIdSuffix::GetMode() const
{
    return Mode_;
}

const std::string& TOutputMessageIdSuffix::GetValue() const
{
    return Value_;
}

std::string TOutputMessageIdSuffix::Resolve(const TMessage& message, i64 sequenceNumber) const
{
    switch (Mode_) {
        case EMode::SequenceNumber:
            return LexicographicallySerialize(sequenceNumber);
        case EMode::PayloadHash: {
            const auto& payload = message.Payload.Underlying();
            std::string buffer(GetWireByteSize(payload), '\0');
            const char* end = SerializeToBuffer(buffer.data(), payload);
            const auto hash = CityHash128(buffer.data(), static_cast<size_t>(end - buffer.data()));
            const ui64 hashParts[] = {Uint128Low64(hash), Uint128High64(hash)};
            auto hex = HexEncode(hashParts, sizeof(hashParts));
            return std::string(hex.data(), hex.size());
        }
        case EMode::UserDefined:
            return LexicographicallySerialize(TStringBuf(Value_));
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

void IOutputCollector::AddMessage(TMessage&& message, TAddMessageOptions options)
{
    DoAddMessage(std::move(message), std::move(options));
}

void IOutputCollector::AddMessage(TMessage&& message, bool distribute)
{
    DoAddMessage(std::move(message), TAddMessageOptions{.Distribute = distribute});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
