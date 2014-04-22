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
    (JobStarted)
    (JobCompleted)
    (JobFailed)
    (JobAborted)
    (OperationCompleted)
    (OperationFailed)
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
        this->Consumer->OnBeginMap();
    }

    TFluentLogEventImpl(TFluentLogEventImpl&& other)
        : TBase(other.Consumer, other.Parent)
    {
        other.Consumer = nullptr;
    }

    ~TFluentLogEventImpl()
    {
        if (this->Consumer) {
            this->Consumer->OnEndMap();
        }
    }

    NYTree::TFluentYsonBuilder::TAny<TThis&&> Item(const TStringBuf& key)
    {
        this->Consumer->OnKeyedItem(key);
        return NYTree::TFluentYsonBuilder::TAny<TThis&&>(this->Consumer, std::move(*this));
    }

};

typedef TFluentLogEventImpl<NYTree::TFluentYsonVoid> TFluentLogEvent;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
