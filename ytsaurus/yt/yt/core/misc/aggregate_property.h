#pragma once

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/serialize.h>

#include <functional>
#include <map>

////////////////////////////////////////////////////////////////////////////////

//! Defines an aggregate property that is set by value. Provides methods
//! 'Get##name', 'Account##name' and 'Discount##name'.
#define DEFINE_BYVAL_AGGREGATE_PROPERTY(aggregator, type, name, ...)

//! Provides aggregate property methods which are delegated to the holder.
#define DEFINE_BYVAL_EXTRA_AGGREGATE_PROPERTY(holder, name)

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TSumAggregate
{
public:
    T Get() const;
    void Account(T value);
    void Discount(T value);
    void AccountDelta(T value);
    void Reset();

    template <class TSaveContext>
    void Save(TSaveContext& context) const;
    template <class TLoadContext>
    void Load(TLoadContext& context);

private:
    T Value_{};
};

template <class T, class TPredicate>
class TComparableAggregateBase
{
public:
    explicit TComparableAggregateBase(T neutralElement);

    T Get() const;
    void Account(T value);
    void Discount(T value);
    void AccountDelta(T value);
    void Reset();

    template <class TSaveContext>
    void Save(TSaveContext& context) const;
    template <class TLoadContext>
    void Load(TLoadContext & context);

private:
    T NeutralElement_;
    std::map<T, i64, TPredicate> Values_;
};

template <class T>
using TMinAggregate = TComparableAggregateBase<T, std::less<int>>;

template <class T>
using TMaxAggregate = TComparableAggregateBase<T, std::greater<int>>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define AGGREGATE_PROPERTY_INL_H
#include "aggregate_property-inl.h"
#undef AGGREGATE_PROPERTY_INL_H
