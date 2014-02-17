#pragma once

#include "public.h"

namespace NYT {
namespace NRpc {

struct IFailureModel
    : public virtual TRefCounted
{
    virtual bool IsRequestFailing() const = 0;
    virtual bool IsResponseFailing() const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IFailureModel)
DECLARE_REFCOUNTED_CLASS(TFailingChannel)

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

DECLARE_REFCOUNTED_CLASS(TBlackHoleFailureModel)

IChannelPtr CreateFailingChannel(
    IChannelPtr underlyingChannel,
    IFailureModelPtr failureModel);

} // namespace NRpc
} // namespae NYT
