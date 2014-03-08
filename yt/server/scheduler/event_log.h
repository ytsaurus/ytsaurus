#pragma once

#include "public.h"

#include <core/misc/enum.h>

#include <core/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ELogEventType,
    (SchedulerStarted)
    (MasterConnected)
    (MasterDisconnected)
);

////////////////////////////////////////////////////////////////////////////////

template <class TParent>
class TFluentLogEventImpl
    : public NYTree::TFluentYsonBuilder::TFluentFragmentBase<TFluentLogEventImpl, TParent>
{
public:
    typedef TFluentLogEventImpl TThis;
    typedef NYTree::TFluentYsonBuilder::TFluentFragmentBase<NScheduler::TFluentLogEventImpl, TParent> TBase;

    explicit TFluentLogEventImpl(NYson::IYsonConsumer* consumer)
        : TBase(consumer)
    {
        Consumer->OnBeginMap();
    }

    TFluentLogEventImpl(TFluentLogEventImpl&& other)
        : TBase(other.Consumer, other.Parent)
    {
        other.Consumer = nullptr;
    }

    ~TFluentLogEventImpl()
    {
        if (Consumer) {
            Consumer->OnEndMap();
        }
    }

    NYTree::TFluentYsonBuilder::TAny<TThis&&> Item(const TStringBuf& key)
    {
        Consumer->OnKeyedItem(key);
        return NYTree::TFluentYsonBuilder::TAny<TThis&&>(this->Consumer, std::move(*this));
    }

};

typedef TFluentLogEventImpl<NYTree::TFluentYsonVoid> TFluentLogEvent;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
