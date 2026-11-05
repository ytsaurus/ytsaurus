#pragma once

#ifndef INTERVAL_H_
    #error "Direct inclusion of this file is not allowed, include interval.h"
    // For the sake of sane code completion.
    #include "interval.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

// Check order, require for TEndpoint comparison.
static_assert(EEndpointType::MinusInf < EEndpointType::KeyMinus);
static_assert(EEndpointType::KeyMinus < EEndpointType::KeyExact);
static_assert(EEndpointType::KeyExact < EEndpointType::KeyPlus);
static_assert(EEndpointType::KeyPlus < EEndpointType::PlusInf);

// Check that the mask works.
static_assert((EEndpointType::MinusInf & EEndpointType::KeyMask) == EEndpointType::KeyNone);
static_assert((EEndpointType::PlusInf & EEndpointType::KeyMask) == EEndpointType::KeyNone);
static_assert((EEndpointType::KeyMinus & EEndpointType::KeyMask) != EEndpointType::KeyNone);
static_assert((EEndpointType::KeyExact & EEndpointType::KeyMask) != EEndpointType::KeyNone);
static_assert((EEndpointType::KeyPlus & EEndpointType::KeyMask) != EEndpointType::KeyNone);

////////////////////////////////////////////////////////////////////////////////

template <class TKey>
constexpr TEndpoint<TKey>::TEndpoint()
    : Data_{std::in_place_index_t<size_t(EEndpointType::SameAsLeft)>{}}
{
    static_assert(std::is_same_v<std::variant_alternative_t<size_t(EEndpointType::MinusInf), TData>, std::monostate>);
    static_assert(std::is_same_v<std::variant_alternative_t<size_t(EEndpointType::KeyMinus), TData>, TKey>);
    static_assert(std::is_same_v<std::variant_alternative_t<size_t(EEndpointType::KeyExact), TData>, TKey>);
    static_assert(std::is_same_v<std::variant_alternative_t<size_t(EEndpointType::KeyPlus), TData>, TKey>);
    static_assert(std::is_same_v<std::variant_alternative_t<size_t(EEndpointType::PlusInf), TData>, std::monostate>);
    static_assert(std::is_same_v<std::variant_alternative_t<size_t(EEndpointType::SameAsLeft), TData>, std::monostate>);
}

template <class TKey>
template <std::convertible_to<TKey> TKeyRef>
constexpr TEndpoint<TKey>::TEndpoint(TKeyRef&& key)
    : Data_{std::in_place_index_t<size_t(EEndpointType::KeyExact)>{}, std::forward<TKeyRef>(key)}
{ }

template <class TKey>
template <std::convertible_to<TKey> TKeyRef>
constexpr TEndpoint<TKey>::TEndpoint(TClosed, TKeyRef&& key)
    : Data_{std::in_place_index_t<size_t(EEndpointType::KeyExact)>{}, std::forward<TKeyRef>(key)}
{ }

template <class TKey>
constexpr TEndpoint<TKey>::TEndpoint(TOpenLeft)
    : Data_{std::in_place_index_t<size_t(EEndpointType::MinusInf)>{}}
{ }

template <class TKey>
constexpr TEndpoint<TKey>::TEndpoint(TOpenRight)
    : Data_{std::in_place_index_t<size_t(EEndpointType::PlusInf)>{}}
{ }

template <class TKey>
constexpr TEndpoint<TKey>::TEndpoint(TOpenLeft, const std::nullopt_t&)
    : Data_{std::in_place_index_t<size_t(EEndpointType::MinusInf)>{}}
{ }

template <class TKey>
constexpr TEndpoint<TKey>::TEndpoint(TOpenRight, const std::nullopt_t&)
    : Data_{std::in_place_index_t<size_t(EEndpointType::PlusInf)>{}}
{ }

template <class TKey>
template <std::convertible_to<TKey> TKeyRef>
constexpr TEndpoint<TKey>::TEndpoint(TOpenLeft, TKeyRef&& key)
    : Data_{std::in_place_index_t<size_t(EEndpointType::KeyPlus)>{}, std::forward<TKeyRef>(key)}
{ }

template <class TKey>
template <std::convertible_to<TKey> TKeyRef>
constexpr TEndpoint<TKey>::TEndpoint(TOpenRight, TKeyRef&& key)
    : Data_{std::in_place_index_t<size_t(EEndpointType::KeyMinus)>{}, std::forward<TKeyRef>(key)}
{ }

template <class TKey>
template <std::convertible_to<std::optional<TKey>> TKeyOptRef>
    requires(!std::convertible_to<TKeyOptRef, TKey> && !std::same_as<std::remove_cvref_t<TKeyOptRef>, std::nullopt_t>)
constexpr TEndpoint<TKey>::TEndpoint(TOpenLeft, TKeyOptRef&& key)
    : Data_{ConstructData<EEndpointType::KeyPlus, EEndpointType::MinusInf>(std::forward<TKeyOptRef>(key))}
{ }

template <class TKey>
template <std::convertible_to<std::optional<TKey>> TKeyOptRef>
    requires(!std::convertible_to<TKeyOptRef, TKey> && !std::same_as<std::remove_cvref_t<TKeyOptRef>, std::nullopt_t>)
constexpr TEndpoint<TKey>::TEndpoint(TOpenRight, TKeyOptRef&& key)
    : Data_{ConstructData<EEndpointType::KeyMinus, EEndpointType::PlusInf>(std::forward<TKeyOptRef>(key))}
{ }

template <class TKey>
constexpr EEndpointType TEndpoint<TKey>::Type() const
{
    return static_cast<EEndpointType>(Data_.index());
}

template <class TKey>
constexpr std::optional<TKey> TEndpoint<TKey>::ExtractKey() const
{
    switch (Data_.index()) {
        case size_t(EEndpointType::KeyMinus):
            return std::get<size_t(EEndpointType::KeyMinus)>(Data_);
        case size_t(EEndpointType::KeyExact):
            return std::get<size_t(EEndpointType::KeyExact)>(Data_);
        case size_t(EEndpointType::KeyPlus):
            return std::get<size_t(EEndpointType::KeyPlus)>(Data_);
        default:
            return std::nullopt;
    }
}

template <class TKey>
constexpr const TKey& TEndpoint<TKey>::GetKeySure() const
{
    switch (Data_.index()) {
        case size_t(EEndpointType::KeyMinus):
            return std::get<size_t(EEndpointType::KeyMinus)>(Data_);
        case size_t(EEndpointType::KeyExact):
            return std::get<size_t(EEndpointType::KeyExact)>(Data_);
        case size_t(EEndpointType::KeyPlus):
            return std::get<size_t(EEndpointType::KeyPlus)>(Data_);
        default:
            YT_UNREACHABLE();
    }
}

template <class TKey>
constexpr std::weak_ordering TEndpoint<TKey>::operator<=>(const TEndpoint<TKey>& rhs) const
{
    const auto& lhs = *this;
    YT_ASSERT(lhs.Type() != EEndpointType::SameAsLeft);
    YT_ASSERT(rhs.Type() != EEndpointType::SameAsLeft);
    if ((lhs.Type() & EEndpointType::KeyMask) == EEndpointType::KeyNone || (rhs.Type() & EEndpointType::KeyMask) == EEndpointType::KeyNone) {
        // At least one of operands is +/- infinity. It's enough to compare types.
        return lhs.Type() <=> rhs.Type();
    }
    // Now both lhs and rhs definitely has keys.
    auto res = DoCompare(lhs.GetKeySure(), rhs.GetKeySure());
    if (res != 0) {
        // If keys are not equal then the result is obvious.
        return res;
    } else {
        // If keys are equal then the defined by order KeyMinus < KeyExact < KeyPlus.
        return lhs.Type() <=> rhs.Type();
    }
}

template <class TKey>
constexpr std::weak_ordering TEndpoint<TKey>::operator<=>(const TKey& rhs) const
{
    const auto& lhs = *this;
    YT_ASSERT(lhs.Type() != EEndpointType::SameAsLeft);
    if ((lhs.Type() & EEndpointType::KeyMask) == EEndpointType::KeyNone) {
        // At least one of operands is +/- infinity. It's enough to compare types.
        return lhs.Type() <=> EEndpointType::KeyExact;
    }
    // Now both lhs and rhs definitely has keys.
    auto res = DoCompare(lhs.GetKeySure(), rhs);
    if (res != 0) {
        // If keys are not equal then the result is obvious.
        return res;
    } else {
        // If keys are equal then the defined by order KeyMinus < KeyExact < KeyPlus.
        return lhs.Type() <=> EEndpointType::KeyExact;
    }
}

template <class TKey>
constexpr std::weak_ordering TEndpoint<TKey>::InvertedCompare(const TKey& lhs, const TEndpoint<TKey>& rhs)
{
    YT_ASSERT(rhs.Type() != EEndpointType::SameAsLeft);
    if ((rhs.Type() & EEndpointType::KeyMask) == EEndpointType::KeyNone) {
        // At least one of operands is +/- infinity. It's enough to compare types.
        return EEndpointType::KeyExact <=> rhs.Type();
    }
    // Now both lhs and rhs definitely has keys.
    auto res = DoCompare(lhs, rhs.GetKeySure());
    if (res != 0) {
        // If keys are not equal then the result is obvious.
        return res;
    } else {
        // If keys are equal then the defined by order KeyMinus < KeyExact < KeyPlus.
        return EEndpointType::KeyExact <=> rhs.Type();
    }
}

template <class TKey>
constexpr bool TEndpoint<TKey>::operator==(const TEndpoint<TKey>& rhs) const
{
    const auto& lhs = *this;
    // Compare types and optionally keys.
    return lhs.Type() == rhs.Type() &&
        (((lhs.Type() & EEndpointType::KeyMask) == EEndpointType::KeyNone) || lhs.GetKeySure() == rhs.GetKeySure());
}

template <class TKey>
constexpr bool TEndpoint<TKey>::operator==(const TKey& rhs) const
{
    const auto& lhs = *this;
    // Compare types and optionally keys.
    return lhs.Type() == EEndpointType::KeyExact && lhs.GetKeySure() == rhs;
}

template <class TKey>
template <EEndpointType ValuedType, EEndpointType ValuelessType, std::convertible_to<std::optional<TKey>> TKeyRef>
constexpr TEndpoint<TKey>::TData TEndpoint<TKey>::ConstructData(TKeyRef&& key)
{
    if (key.has_value()) {
        return TData{std::in_place_index_t<size_t(ValuedType)>{}, std::forward<TKeyRef>(key).value()};
    } else {
        return TData{std::in_place_index_t<size_t(ValuelessType)>{}};
    }
}

template <class TKey>
void FormatValue(TStringBuilderBase* builder, const TEndpoint<TKey>& endpoint, TStringBuf /*spec*/)
{
    switch (endpoint.Data_.index()) {
        case size_t(EEndpointType::MinusInf):
            builder->AppendFormat("-inf");
            break;
        case size_t(EEndpointType::KeyMinus):
            builder->AppendFormat("%v-", endpoint.GetKeySure());
            break;
        case size_t(EEndpointType::KeyExact):
            builder->AppendFormat("%v", endpoint.GetKeySure());
            break;
        case size_t(EEndpointType::KeyPlus):
            builder->AppendFormat("%v+", endpoint.GetKeySure());
            break;
        case size_t(EEndpointType::PlusInf):
            builder->AppendFormat("+inf");
            break;
        case size_t(EEndpointType::SameAsLeft):
            builder->AppendFormat("==");
            break;
        default:
            YT_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey>
template <std::convertible_to<TKey> TKeyRef>
constexpr TInterval<TKey>::TInterval(TKeyRef&& key)
    : Left_{std::forward<TKeyRef>(key)}
    , Right_{}
{ }

template <class TKey>
template <std::convertible_to<std::optional<TKey>> TLeft, std::convertible_to<std::optional<TKey>> TRight>
constexpr TInterval<TKey>::TInterval(TLeft&& left, TRight&& right)
    : Left_{TOpenLeft{}, std::forward<TLeft>(left)}
    , Right_{TOpenRight{}, std::forward<TRight>(right)}
{ }

template <class TKey>
constexpr const TEndpoint<TKey>& TInterval<TKey>::EffectiveLeft() const
{
    return Left_;
}

template <class TKey>
const TEndpoint<TKey>& TInterval<TKey>::EffectiveRight() const
{
    if (Right_.Type() == EEndpointType::SameAsLeft) {
        YT_ASSERT(Left_.Type() == EEndpointType::KeyExact);
    }
    return Right_.Type() == EEndpointType::SameAsLeft ? Left_ : Right_;
}

template <class TKey>
std::optional<TKey> TInterval<TKey>::ExtractLeft() const
{
    return Left_.ExtractKey();
}

template <class TKey>
std::optional<TKey> TInterval<TKey>::ExtractRight() const
{
    return Right_.ExtractKey();
}

template <class TKey>
void TInterval<TKey>::SwapRight(TInterval<TKey>& that)
{
    YT_ASSERT(Right_.Type() != EEndpointType::SameAsLeft);
    YT_ASSERT(that.Right_.Type() != EEndpointType::SameAsLeft);
    std::swap(Right_, that.Right_);
}

template <class TKey>
void FormatValue(TStringBuilderBase* builder, const TInterval<TKey>& interval, TStringBuf /*spec*/)
{
    switch (interval.EffectiveLeft().Type()) {
        case EEndpointType::MinusInf:
            builder->AppendFormat("(-inf, ");
            break;
        case EEndpointType::KeyExact:
            builder->AppendFormat("[%v, ", interval.EffectiveLeft().GetKeySure());
            break;
        case EEndpointType::KeyPlus:
            builder->AppendFormat("(%v, ", interval.EffectiveLeft().GetKeySure());
            break;
        default:
            YT_UNREACHABLE();
    };
    switch (interval.EffectiveRight().Type()) {
        case EEndpointType::PlusInf:
            builder->AppendFormat("+inf)");
            break;
        case EEndpointType::KeyExact:
            builder->AppendFormat("%v]", interval.EffectiveRight().GetKeySure());
            break;
        case EEndpointType::KeyMinus:
            builder->AppendFormat("%v)", interval.EffectiveRight().GetKeySure());
            break;
        case EEndpointType::SameAsLeft:
            builder->AppendFormat("%v]", interval.EffectiveLeft().GetKeySure());
            break;
        default:
            YT_UNREACHABLE();
    };
}

template <class TKey>
template <class TLeft, class TRight>
constexpr std::weak_ordering TEndpoint<TKey>::DoCompare(const TLeft& lhs, const TRight& rhs)
{
    if constexpr (CSupportsSpaceshipOperator<TLeft, TRight>) {
        return lhs <=> rhs;
    } else {
        if (lhs == rhs) {
            return std::weak_ordering::equivalent;
        }
        return lhs < rhs ? std::weak_ordering::less : std::weak_ordering::greater;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
