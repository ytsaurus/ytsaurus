#pragma once

#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IFailureModel)

struct IFailureModel
    : public virtual TRefCounted
{
    virtual bool IsRequestFailing() const = 0;
    virtual bool IsResponseFailing() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFailureModel)

////////////////////////////////////////////////////////////////////////////////

class TBlackHoleFailureModel
    : public IFailureModel
{
public:
    TBlackHoleFailureModel()
        : IsRequestFailing_(false)
        , IsResponseFailing_(false)
    { }

    virtual bool IsRequestFailing() const override
    {
        return IsRequestFailing_;
    }

    void SetRequestFailing(bool requestFailing)
    {
        IsRequestFailing_ = requestFailing;
    }

    virtual bool IsResponseFailing() const override
    {
        return IsResponseFailing_;
    }

    void SetResponseFailing(bool responseFailing)
    {
        IsResponseFailing_ = responseFailing;
    }

private:
    bool IsRequestFailing_;
    bool IsResponseFailing_;
};

typedef TIntrusivePtr<TBlackHoleFailureModel> TBlackHoleFailureModelPtr;

DEFINE_REFCOUNTED_TYPE(TBlackHoleFailureModel)

IChannelPtr CreateFailingChannel(
    IChannelPtr underlyingChannel,
    IFailureModelPtr failureModel);

} // namespace NRpc
} // namespae NYT
