#ifndef AGGREGATE_PROPERTY_INL_H
#error "Direct inclusion of this file is not allowed, include aggregate_property.h"
// For the sake of sane code completion.
#include "aggregate_property.h"
#endif

////////////////////////////////////////////////////////////////////////////////

#undef DEFINE_BYVAL_AGGREGATE_PROPERTY
#undef DEFINE_BYVAL_EXTRA_AGGREGATE_PROPERTY

#define DEFINE_AGGREGATE_PROPERTY_IMPL(aggregator, type, name, ...) \
protected: \
    aggregator<type> name##_ { __VA_ARGS__ }; \
    \
public: \
    void Account##name(const type& value) \
    { \
        name##_.Account(value); \
    } \
    \
    void Discount##name(const type& value) \
    { \
        name##_.Discount(value); \
    } \
    \
    void Account##name##Delta(const type& value) \
    { \
        name##_.AccountDelta(value); \
    } \
    \
    void Reset##name() \
    { \
    name##_.Reset(); \
    } \

#define DEFINE_BYVAL_AGGREGATE_PROPERTY(aggregator, type, name, ...) \
    DEFINE_AGGREGATE_PROPERTY_IMPL(aggregator, type, name, __VA_ARGS__) \
    type Get##name() const \
    { \
        return name##_.Get(); \
    } \
    static_assert(true)

#define DEFINE_BYVAL_EXTRA_AGGREGATE_PROPERTY(holder, name) \
public: \
    Y_FORCE_INLINE decltype(holder##_->name.Get()) Get##name() const \
    { \
        if (!holder##_) { \
            return Default##holder##_.name.Get(); \
        } \
        return holder##_->name.Get(); \
    } \
    Y_FORCE_INLINE void Account##name(decltype(holder##_->name.Get()) val) \
    { \
        INITIALIZE_EXTRA_PROPERTY_HOLDER(holder); \
        holder##_->name.Account(val); \
    } \
    Y_FORCE_INLINE void Discount##name(decltype(holder##_->name.Get()) val) \
    { \
        INITIALIZE_EXTRA_PROPERTY_HOLDER(holder); \
        holder##_->name.Discount(val); \
    } \
    Y_FORCE_INLINE void Account##name##Delta(decltype(holder##_->name.Get()) val) \
    { \
        INITIALIZE_EXTRA_PROPERTY_HOLDER(holder); \
        holder##_->name.AccountDelta(val); \
    } \
    Y_FORCE_INLINE void Reset##name() \
    { \
        INITIALIZE_EXTRA_PROPERTY_HOLDER(holder); \
        holder##_->name.Reset(); \
    } \
    static_assert(true)

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T TSumAggregate<T>::Get() const
{
    return Value_;
}

template <class T>
void TSumAggregate<T>::Account(T value)
{
    Value_ += value;
}

template <class T>
void TSumAggregate<T>::Discount(T value)
{
    Value_ -= value;
}

template <class T>
void TSumAggregate<T>::AccountDelta(T delta)
{
    Value_ += delta;
}

template <class T>
void TSumAggregate<T>::Reset()
{
    Value_ = T{};
}

template <class T>
template <class TSaveContext>
void TSumAggregate<T>::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Value_);
}

template <class T>
template <class TLoadContext>
void TSumAggregate<T>::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, Value_);
}

template <class T, class TPredicate>
TComparableAggregateBase<T, TPredicate>::TComparableAggregateBase(T neutralElement)
    : NeutralElement_(std::move(neutralElement))
{ }

template <class T, class TPredicate>
T TComparableAggregateBase<T, TPredicate>::Get() const
{
    return Values_.empty() ? NeutralElement_ : Values_.begin()->first;
}

template <class T, class TPredicate>
void TComparableAggregateBase<T, TPredicate>::Account(T value)
{
    if (value == NeutralElement_) {
        return;
    }

    ++Values_[value];
}

template <class T, class TPredicate>
void TComparableAggregateBase<T, TPredicate>::Discount(T value)
{
    if (value == NeutralElement_) {
        return;
    }

    auto it = Values_.find(value);
    YT_VERIFY(it != Values_.end());
    YT_VERIFY(it->second > 0);
    if (--it->second == 0) {
        Values_.erase(it);
    }
}

template <class T, class TPredicate>
void TComparableAggregateBase<T, TPredicate>::AccountDelta(T /*value*/)
{
    YT_ABORT();
}

template <class T, class TPredicate>
void TComparableAggregateBase<T, TPredicate>::Reset()
{
    Values_.clear();
}

template <class T, class TPredicate>
template <class TSaveContext>
void TComparableAggregateBase<T, TPredicate>::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, NeutralElement_);
    Save(context, Values_);
}

template <class T, class TPredicate>
template <class TLoadContext>
void TComparableAggregateBase<T, TPredicate>::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, NeutralElement_);
    Load(context, Values_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
