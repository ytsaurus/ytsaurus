#ifndef EVENT_LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include event_log.h"
// For the sake of sane code completion.
#include "event_log.h"
#endif

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
TFluentLogEventImpl<TParent>::TFluentLogEventImpl(std::unique_ptr<NYson::IYsonConsumer> consumer)
    : TBase(consumer.get())
    , Consumer_(std::move(consumer))
{
    Consumer_->OnBeginMap();
}

template <class TParent>
TFluentLogEventImpl<TParent>::~TFluentLogEventImpl()
{
    if (Consumer_) {
        Consumer_->OnEndMap();
    }
}

template <class TParent>
NYTree::TFluentYsonBuilder::TAny<TFluentLogEventImpl<TParent>&&> TFluentLogEventImpl<TParent>::Item(TStringBuf key)
{
    Consumer_->OnKeyedItem(key);
    return NYTree::TFluentYsonBuilder::TAny<TThis&&>(Consumer_.get(), std::move(*this));
}

template <class TParent>
TFluentLogEventImpl<TParent>::TThis& TFluentLogEventImpl<TParent>::Items(const NYson::TYsonString& items)
{
    YT_VERIFY(items.GetType() == NYson::EYsonType::MapFragment);
    Consumer_->OnRaw(items);
    return *this;
}

template <class TParent>
TFluentLogEventImpl<TParent>::TThis& TFluentLogEventImpl<TParent>::OptionalItem(
    TStringBuf key,
    const auto& optionalValue,
    auto&&... extraArgs)
{
    using NYTree::Serialize;

    if (optionalValue) {
        this->Consumer_->OnKeyedItem(key);
        Serialize(optionalValue, this->Consumer_.get(), std::forward<decltype(extraArgs)>(extraArgs)...);
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
