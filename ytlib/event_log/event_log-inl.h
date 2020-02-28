#pragma once
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
    return NYTree::TFluentYsonBuilder::TAny<TThis&&>(this->Consumer, std::move(*this));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
