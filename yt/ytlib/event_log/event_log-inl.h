#pragma once
#ifndef EVENT_LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include event_log.h"
// For the sake of sane code completion.
#include "event_log.h"
#endif

namespace NYT::NEventLog {

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
TFluentLogEventImpl<TParent>::TFluentLogEventImpl(TFluentEventLogger* logger)
    : TBase(logger->Consumer_)
    , Logger_(logger)
{
    Acquire();
}

template <class TParent>
TFluentLogEventImpl<TParent>::TFluentLogEventImpl(NYson::IYsonConsumer* consumer)
    : TBase(consumer)
    , Logger_(nullptr)
{ }

template <class TParent>
TFluentLogEventImpl<TParent>::TFluentLogEventImpl(TFluentLogEventImpl<TParent>&& other)
    : TBase(other.Consumer, other.Parent)
    , Logger_(other.Logger_)
{
    other.Logger_ = nullptr;
}

template <class TParent>
TFluentLogEventImpl<TParent>::TFluentLogEventImpl(const TFluentLogEventImpl<TParent>& other)
    : TBase(other.Consumer, other.Parent)
    , Logger_(other.Logger_)
{
    Acquire();
}

template <class TParent>
TFluentLogEventImpl<TParent>::~TFluentLogEventImpl()
{
    Release();
}

template <class TParent>
NYTree::TFluentYsonBuilder::TAny<TFluentLogEventImpl<TParent>&&> TFluentLogEventImpl<TParent>::Item(TStringBuf key)
{
    this->Consumer->OnKeyedItem(key);
    return NYTree::TFluentYsonBuilder::TAny<TThis&&>(this->Consumer, std::move(*this));
}

template <class TParent>
void TFluentLogEventImpl<TParent>::Acquire()
{
    if (this->Logger_) {
        this->Logger_->Acquire();
    }
}

template <class TParent>
void TFluentLogEventImpl<TParent>::Release()
{
    if (this->Logger_) {
        this->Logger_->Release();
        this->Logger_ = nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NEventLog
