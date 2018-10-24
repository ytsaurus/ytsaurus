#pragma once
#ifndef FLUENT_LOG_INL_H_
#error "Direct inclusion of this file is not allowed, include log.h"
// For the sake of sane code completion
#include "log.h"
#include "fluent_log.h" // it makes CLion happy
#endif
#undef FLUENT_LOG_INL_H_

namespace NYT {
namespace NLogging {

/////////////////////////////////////////////////////////////////////////////////////////////

template <class TParent>
TOneShotFluentLogEventImpl<TParent>::TOneShotFluentLogEventImpl(TStatePtr state, const NLogging::TLogger& logger,
    NLogging::ELogLevel level)
    : TBase(state->GetConsumer())
    , State_(std::move(state))
    , Logger_(logger)
    , Level_(level)
{ }

template <class TParent>
NYTree::TFluentYsonBuilder::TAny<TOneShotFluentLogEventImpl<TParent>&&> TOneShotFluentLogEventImpl<TParent>::Item(TStringBuf key)
{
    this->Consumer->OnKeyedItem(key);
    return NYTree::TFluentYsonBuilder::TAny<TThis&&>(this->Consumer, std::move(*this));
}

template <class TParent>
TOneShotFluentLogEventImpl<TParent>::~TOneShotFluentLogEventImpl()
{
    LogStructuredEvent(Logger_, State_->GetValue(), Level_);
}
/////////////////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
