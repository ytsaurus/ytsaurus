#include "attribute_policy.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

constexpr size_t RandomStringLength = 16;
constexpr TStringBuf AvailableChars = "0123456789abcdefghijklmnopqrstuvwxyz";

////////////////////////////////////////////////////////////////////////////////

class TManualStringAttributePolicy
    : public TManualAttributePolicy<TString>
{
public:
    TManualStringAttributePolicy(
        size_t minLength,
        size_t maxLength,
        std::string_view charset)
        : MinLength_(minLength)
        , MaxLength_(maxLength)
        , Bitmap_(MakeBitmap(charset))
    {
        if (minLength > maxLength) {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "Attribute length range is unusable: minimum %v > maximum %v",
                minLength,
                maxLength);
        }

        // TODO(deep): this is only for keys. Make another round of policy refactoring so that
        // all the key stuff is separated from the generic stuff.

        // | is used as the fqid separator
        // ; is used as the object key separator
        for (char c : {FqidSeparator, CompositeKeySeparator}) {
            if (CheckChar(c)) {
                THROW_ERROR_EXCEPTION(
                    NClient::EErrorCode::InvalidObjectId,
                    "Character %Qv is reserved and cannot be a part of an object id", c);
            }
        }
    }

    void Validate(const TString& value, std::string_view title) const override
    {
        if (value.size() < MinLength_ || MaxLength_ < value.size()) {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "%v %Qv has length %v, expected [%v, %v]",
                title,
                value,
                value.size(),
                MinLength_,
                MaxLength_);
        }

        for (char chr : value) {
            if (!CheckChar(chr)) {
                THROW_ERROR_EXCEPTION(
                    NClient::EErrorCode::InvalidObjectId,
                    "%v %Qv contains invalid symbol %Qv",
                    title,
                    value,
                    chr);
            }
        }
    }

    bool CheckChar(char chr) const
    {
        int code = static_cast<unsigned char>(chr);
        return Bitmap_[code];
    }

private:
    const size_t MinLength_;
    const size_t MaxLength_;
    const std::vector<bool> Bitmap_;

    inline static const int MinAllowedChar_ = 1;
    inline static const int MaxAllowedChar_ = 127;

    static std::vector<bool> MakeBitmap(std::string_view charset)
    {
        std::vector<bool> result(256, false);
        for (char chr : charset) {
            int code = static_cast<unsigned char>(chr);
            if (code < MinAllowedChar_ || MaxAllowedChar_ < code) {
                THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "Character %Qv is out of the valid range for an object id",
                chr);
            }
            result[code] = true;
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRandomStringAttributePolicy
    : public TManualStringAttributePolicy
{
public:
    TRandomStringAttributePolicy(
        size_t minLength,
        size_t maxLength,
        std::string_view charset)
        : TManualStringAttributePolicy(minLength, maxLength, charset)
    {
        for (char chr : AvailableChars) {
            if (!CheckChar(chr)) {
                THROW_ERROR_EXCEPTION(
                    NClient::EErrorCode::InvalidObjectId,
                    "Character %Qv should be allowed for autogeneration of attribute",
                    chr);
            }
        }
        if (RandomStringLength < minLength || maxLength < RandomStringLength) {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "Length %v should belong to range [%v; %v] for autogeneration of attribute",
                RandomStringLength,
                minLength,
                maxLength);
        }
    }

    EAttributeGenerationPolicy GetGenerationPolicy() const override
    {
        return EAttributeGenerationPolicy::Random;
    }

    TString Generate(TTransaction*, std::string_view) const override
    {
        return RandomStringId();
    }
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<IAttributePolicy<TString>> CreateStringAttributePolicy(
    EAttributeGenerationPolicy policy,
    size_t minLength,
    size_t maxLength,
    std::string_view charset)
{
    TIntrusivePtr<IAttributePolicy<TString>> result;
    switch (policy) {
        case EAttributeGenerationPolicy::Manual:
            result = New<NDetail::TManualStringAttributePolicy>(minLength, maxLength, charset);
            break;

        case EAttributeGenerationPolicy::Random:
            result = New<NDetail::TRandomStringAttributePolicy>(minLength, maxLength, charset);
            break;

        default:
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "Invalid generation policy for string attribute: %v",
                policy);
    }

    YT_VERIFY(result->GetGenerationPolicy() == policy);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TString RandomStringId()
{
    TStringBuilder builder;
    for (size_t index = 0; index != NDetail::RandomStringLength; ++index) {
        builder.AppendChar(
            NDetail::AvailableChars[RandomNumber<size_t>(NDetail::AvailableChars.size())]);
    }
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
