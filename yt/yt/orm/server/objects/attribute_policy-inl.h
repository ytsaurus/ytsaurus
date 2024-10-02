#ifndef ATTRIBUTE_POLICY_INL_H_
#error "Direct inclusion of this file is not allowed, include attribute_policy.h"
// For the sake of sane code completion.
#include "attribute_policy.h"
#endif

#include "attribute_policy_detail.h"
#include "transaction.h"
#include "select_query_executor.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TValue>
class TManualAttributePolicy
    : public IAttributePolicy<TValue>
{
public:
    EAttributeGenerationPolicy GetGenerationPolicy() const override
    {
        return EAttributeGenerationPolicy::Manual;
    }

    TValue Generate(TTransaction*, std::string_view title) const override
    {
        THROW_ERROR_EXCEPTION(
            NClient::EErrorCode::InvalidObjectId,
            "%v must be specified",
            title);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TInteger>
class TManualIntegerAttributePolicy
    : public TManualAttributePolicy<TInteger>
{
public:
    TManualIntegerAttributePolicy(
        TInteger minValue,
        TInteger maxValue)
        : MinValue_(minValue)
        , MaxValue_(maxValue)
    {
        if (MinValue_ > MaxValue_) {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "minValue <= maxValue must hold, but instead got %v > %v",
                MinValue_,
                MaxValue_);
        }
    }

    void Validate(
        const TInteger& value,
        std::string_view title) const override
    {
        if (value < MinValue_ || MaxValue_ < value) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidObjectId,
                "%v %v does not belong to the range [%v; %v]",
                title,
                value,
                MinValue_,
                MaxValue_);
        }
    }

protected:
    const TInteger MinValue_;
    const TInteger MaxValue_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TInteger>
class TEntropyIntegerAttributePolicy
    : public TManualIntegerAttributePolicy<TInteger>
{
public:
    TEntropyIntegerAttributePolicy(
        std::unique_ptr<IEntropySource> entropySource,
        TInteger minValue,
        TInteger maxValue)
        : TManualIntegerAttributePolicy<TInteger>(minValue, maxValue)
        , EntropySource_(std::move(entropySource))
    {
        ui64 minGeneratedValue = EntropySource_->GetMinValue();
        ui64 maxGeneratedValue = EntropySource_->GetMaxValue();
        // Handle signed types carefully.
        if ((minValue >= 0 && minGeneratedValue < static_cast<ui64>(minValue)) ||
            (maxValue < 0 || static_cast<ui64>(maxValue) < maxGeneratedValue))
        {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "Validation range [%v; %v] should contain autogeneration range [%v; %v]",
                minValue,
                maxValue,
                minGeneratedValue,
                maxGeneratedValue);
        }
    }

    EAttributeGenerationPolicy GetGenerationPolicy() const override
    {
        return EntropySource_->GetGenerationPolicy();
    }

    TInteger Generate(TTransaction*, std::string_view) const override
    {
        return static_cast<TInteger>(EntropySource_->Get());
    }

private:
    std::unique_ptr<IEntropySource> EntropySource_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TInteger>
class TIndexedIncrementIntegerAttributePolicy
    : public TManualIntegerAttributePolicy<TInteger>
{
public:
    TIndexedIncrementIntegerAttributePolicy(
        TInteger minValue,
        TInteger maxValue,
        TAttributePolicyOptions options)
        : TManualIntegerAttributePolicy<TInteger>(minValue, maxValue)
        , Type_(options.Type)
        , SelectOptions_(TSelectQueryOptions{.Limit = 1, .CheckReadPermissions = true})
    {
        Selector_.Paths.push_back(options.AttributePath);
        OrderBy_.Expressions.push_back(TObjectOrderByExpression{
            .Expression = options.AttributePath,
            .Descending = true,
        });
        if (!options.IndexForIncrement.empty()) {
            Filter_ = TObjectFilter{.Query = Format("[%v]!=#", options.AttributePath)};
            Index_ = TIndex{.Name = std::move(options.IndexForIncrement)};
        }
    }

    EAttributeGenerationPolicy GetGenerationPolicy() const override
    {
        return EAttributeGenerationPolicy::IndexedIncrement;
    }

    TInteger Generate(TTransaction* transaction, std::string_view title) const override
    {
        try {
            auto selectQueryExecutor = MakeSelectQueryExecutor(
                transaction,
                Type_,
                Filter_,
                Selector_,
                OrderBy_,
                SelectOptions_,
                Index_);

            auto result = std::move(*selectQueryExecutor).Execute();
            if (result.Objects.empty()) {
                return TManualIntegerAttributePolicy<TInteger>::MinValue_;
            }
            YT_VERIFY(result.Objects.size() == 1);
            YT_VERIFY(result.Objects[0].Values.size() == 1);

            auto value = NYTree::ConvertTo<TInteger>(result.Objects[0].Values[0]) + 1;
            if (value > TManualIntegerAttributePolicy<TInteger>::MaxValue_) {
                THROW_ERROR_EXCEPTION("Generated value %v is too large", value);
            }
            return value;
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Failed to generate an indexed incremented value for %v",
                title)
                << ex;
        }
    }

private:
    const TObjectTypeValue Type_;
    const TSelectQueryOptions SelectOptions_;

    std::optional<TObjectFilter> Filter_;
    TAttributeSelector Selector_;
    TObjectOrderBy OrderBy_;
    std::optional<TIndex> Index_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <typename TInteger>
TIntrusivePtr<IAttributePolicy<TInteger>> CreateIntegerAttributePolicy(
    EAttributeGenerationPolicy policy,
    NMaster::IBootstrap* bootstrap,
    TInteger minValue,
    TInteger maxValue,
    TAttributePolicyOptions options)
{
    static_assert(std::is_integral_v<TInteger>);

    TIntrusivePtr<IAttributePolicy<TInteger>> result;
    switch (policy) {
        case EAttributeGenerationPolicy::Manual:
            result = New<NDetail::TManualIntegerAttributePolicy<TInteger>>(minValue, maxValue);
            break;

        case EAttributeGenerationPolicy::Random:
            result = New<NDetail::TEntropyIntegerAttributePolicy<TInteger>>(
                NDetail::MakeRandomEntropySource(),
                minValue,
                maxValue);
            break;

        case EAttributeGenerationPolicy::Timestamp:
            result = New<NDetail::TEntropyIntegerAttributePolicy<TInteger>>(
                NDetail::MakeTimestampEntropySource(bootstrap),
                minValue,
                maxValue);
            break;

        case EAttributeGenerationPolicy::BufferedTimestamp:
            result = New<NDetail::TEntropyIntegerAttributePolicy<TInteger>>(
                NDetail::MakeBufferedTimestampEntropySource(bootstrap),
                minValue,
                maxValue);
            break;

        case EAttributeGenerationPolicy::IndexedIncrement:
            result = New<NDetail::TIndexedIncrementIntegerAttributePolicy<TInteger>>(
                minValue,
                maxValue,
                options);
            break;

        default:
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "Invalid generation policy for integer attribute policy: %v",
                policy);
    }

    YT_VERIFY(result->GetGenerationPolicy() == policy);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
