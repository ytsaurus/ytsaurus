#pragma once

#include <yt/core/yson/public.h>
#include <yt/core/ytree/fluent.h>

namespace NYT::NLogging {

/////////////////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TOneShotFluentLogEventImpl
    : public NYTree::TFluentYsonBuilder::TFluentFragmentBase<TOneShotFluentLogEventImpl, TParent, NYTree::TFluentMap>
{
public:
    using TThis = TOneShotFluentLogEventImpl;
    using TBase = NYTree::TFluentYsonBuilder::TFluentFragmentBase<NLogging::TOneShotFluentLogEventImpl, TParent, NYTree::TFluentMap>;
    using TStatePtr = TIntrusivePtr<NYTree::TFluentYsonWriterState>;

    TOneShotFluentLogEventImpl(TStatePtr state, const NLogging::TLogger& logger, NLogging::ELogLevel level);

    TOneShotFluentLogEventImpl(TOneShotFluentLogEventImpl&& other) = default;

    TOneShotFluentLogEventImpl(const TOneShotFluentLogEventImpl& other) = delete;

    TOneShotFluentLogEventImpl& operator=(TOneShotFluentLogEventImpl&& other) = delete;

    TOneShotFluentLogEventImpl& operator=(const TOneShotFluentLogEventImpl& other) = delete;

    NYTree::TFluentYsonBuilder::TAny<TThis&&> Item(TStringBuf key);

    ~TOneShotFluentLogEventImpl();
private:
    TStatePtr State_;
    const NLogging::TLogger& Logger_;
    NLogging::ELogLevel Level_;
};

/////////////////////////////////////////////////////////////////////////////////////////////

using TOneShotFluentLogEvent = TOneShotFluentLogEventImpl<NYTree::TFluentYsonVoid>;

TOneShotFluentLogEvent LogStructuredEventFluently(const NLogging::TLogger& logger, NLogging::ELogLevel level);

/////////////////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

#define FLUENT_LOG_INL_H_
#include "fluent_log-inl.h"
#undef FLUENT_LOG_INL_H_
