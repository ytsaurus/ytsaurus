#pragma once

#include "public.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TLeft, class TRight>
concept CSupportsSpaceshipOperator = requires(const TLeft& a, const TRight& b) {
    a <=> b;
};

////////////////////////////////////////////////////////////////////////////////

//! Type of endpoint.
DEFINE_AMBIGUOUS_BIT_ENUM_WITH_UNDERLYING_TYPE(EEndpointType, unsigned int,
    //! Minus infinity.
    ((MinusInf)(0))
    //! Endpoint equal to some key minus infinitesimal. Left one-sided limit of some key.
    ((KeyMinus)(1))
    //! Endpoint exactly equal to some key.
    ((KeyExact)(2))
    //! Endpoint equal to some key plus infinitesimal. Right one-sided limit of some key.
    ((KeyPlus)(3))
    //! Mask with bits of key-containing endpoints (Key{Minus|Exact|Plus}).
    ((KeyMask)(3))
    //! Mask with no bits, just for pretty comparison.
    ((KeyNone)(0))
    //! Plus infinity.
    ((PlusInf)(4))
    //! Special type for right endpoint of degenerate interval [x, x]. The right key is equal to left and is not stored in that case.
    ((SameAsLeft)(5)));

////////////////////////////////////////////////////////////////////////////////

//! Type of dummy parameter of TEndpoint constructor that specifies that the endpoint is closed.
struct TEndpointClosed
{ };

//! Type of dummy parameter of TEndpoint constructor that specifies that the endpoint is open left endpoint of some interval.
struct TEndpointOpenLeft
{ };

//! Type of dummy parameter of TEndpoint constructor that specifies that the endpoint is open right endpoint of some interval.
struct TEndpointOpenRight
{ };

////////////////////////////////////////////////////////////////////////////////

//! Left or right endpoint of an interval. Allows to store open/closed endpoint along with +-infinity.
//! Is designed to provide simple and optimal comparison of both left and right endpoints with any key or between each other,
//! so for any kind of interval (open/closed/etc) and any key the key belongs to the interval iff left <= key <= right.
template <class TKey>
class TEndpoint
{
private:
    using TData = std::variant<std::monostate, TKey, TKey, TKey, std::monostate, std::monostate>;
    using TClosed = TEndpointClosed;
    using TOpenLeft = TEndpointOpenLeft;
    using TOpenRight = TEndpointOpenRight;

    TData Data_;

public:
    //! Construct with special type (EEndpointType::SameAsLeft) by default.
    constexpr TEndpoint();

    //! Construct endpoint equal to key.
    template <std::convertible_to<TKey> TKeyRef>
    explicit constexpr TEndpoint(TKeyRef&& key);

    //! The same: construct endpoint equal to key.
    template <std::convertible_to<TKey> TKeyRef>
    constexpr TEndpoint(TClosed, TKeyRef&& key);

    //! Construct minus infinity endpoint - left endpoint of open range (-inf, ..).
    explicit constexpr TEndpoint(TOpenLeft);

    //! Construct plus infinity endpoint - right endpoint of open range (.., +inf).
    explicit constexpr TEndpoint(TOpenRight);

    //! Construct minus infinity endpoint - left endpoint of open range (-inf, ..).
    constexpr TEndpoint(TOpenLeft, const std::nullopt_t&);

    //! Construct plus infinity endpoint - right endpoint of open range (.., +inf).
    constexpr TEndpoint(TOpenRight, const std::nullopt_t&);

    //! Construct left endpoint of open range (key, ..).
    template <std::convertible_to<TKey> TKeyRef>
    constexpr TEndpoint(TOpenLeft, TKeyRef&& key);

    //! Construct right endpoint of open range (.., key).
    template <std::convertible_to<TKey> TKeyRef>
    constexpr TEndpoint(TOpenRight, TKeyRef&& key);

    //! Construct general open left endpoint of some range, optionally with minus infinity or particular key.
    template <std::convertible_to<std::optional<TKey>> TKeyOptRef>
        requires(!std::convertible_to<TKeyOptRef, TKey> && !std::same_as<std::remove_cvref_t<TKeyOptRef>, std::nullopt_t>)
    constexpr TEndpoint(TOpenLeft, TKeyOptRef&& key);

    //! Construct general open right endpoint of some range, optionally with minus infinity or particular key.
    template <std::convertible_to<std::optional<TKey>> TKeyOptRef>
        requires(!std::convertible_to<TKeyOptRef, TKey> && !std::same_as<std::remove_cvref_t<TKeyOptRef>, std::nullopt_t>)
    constexpr TEndpoint(TOpenRight, TKeyOptRef&& key);

    //! Get type of endpoint.
    constexpr EEndpointType Type() const;

    //! Get endpoint key, optionally (nullopt for +- inf or special type).
    constexpr std::optional<TKey> ExtractKey() const;

    //! Get endpoint key, having known for sure that it's defined (so it's definitely not +-inf or a special type).
    //! If not sure - better use ExtractKey to avoid UB.
    constexpr const TKey& GetKeySure() const;

    //! Basic comparison.
    constexpr std::weak_ordering operator<=>(const TEndpoint<TKey>& rhs) const;
    constexpr bool operator==(const TEndpoint<TKey>& rhs) const;

    //! Comparison with TKey.
    constexpr std::weak_ordering operator<=>(const TKey& rhs) const;
    constexpr bool operator==(const TKey& rhs) const;

    friend constexpr std::weak_ordering operator<=>(const TKey& lhs, const TEndpoint<TKey>& rhs)
    {
        return InvertedCompare(lhs, rhs);
    }

    friend constexpr bool operator==(const TKey& lhs, const TEndpoint<TKey>& rhs)
    {
        return rhs == lhs;
    }

    template <class TAnyKey>
    friend void FormatValue(TStringBuilderBase* builder, const TEndpoint<TAnyKey>& endpoint, TStringBuf /*spec*/);

private:
    template <class TLeft, class TRight>
    static constexpr std::weak_ordering DoCompare(const TLeft& lhs, const TRight& rhs);
    template <EEndpointType ValuedType, EEndpointType ValuelessType, std::convertible_to<std::optional<TKey>> TKeyRef>
    static constexpr TData ConstructData(TKeyRef&& key);
    static constexpr std::weak_ordering InvertedCompare(const TKey& lhs, const TEndpoint<TKey>& rhs);
};

////////////////////////////////////////////////////////////////////////////////

//! Abstract key interval. Logically can be open, closed, half-open, bounded, unbounded - any kind,
//! but now limited so only open (a, b) and degenerate [a, a] can be constructed.
template <class TKey>
class TInterval
{
private:
    using TOpenLeft = TEndpointOpenLeft;
    using TOpenRight = TEndpointOpenRight;

    TEndpoint<TKey> Left_;
    TEndpoint<TKey> Right_;

public:
    //! Construct degenerate (point) interval [key, key].
    template <std::convertible_to<TKey> TKeyRef>
    explicit constexpr TInterval(TKeyRef&& key);

    //! Construct open interval (|left|, |right|). If std::nullopt as |left| / |right| mean -/+ infinity endpoint.
    template <std::convertible_to<std::optional<TKey>> TLeft, std::convertible_to<std::optional<TKey>> TRight>
    constexpr TInterval(TLeft&& left, TRight&& right);

    //! Get left comparable endpoint.
    constexpr const TEndpoint<TKey>& EffectiveLeft() const;

    //! Get right comparable endpoint.
    const TEndpoint<TKey>& EffectiveRight() const;

    //! Get left key if present.
    std::optional<TKey> ExtractLeft() const;

    //! Get right key if present. Note that the key is not present for an interval created as degenerate.
    std::optional<TKey> ExtractRight() const;

    //! Swap right endpoint with another interval. Must not be applied for degenerate intervals.
    void SwapRight(TInterval<TKey>& that);

    template <class TAnyKey>
    friend void FormatValue(TStringBuilderBase* builder, const TInterval<TAnyKey>& interval, TStringBuf /*spec*/);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define INTERVAL_H_
#include "interval-inl.h"
#undef INTERVAL_H_
