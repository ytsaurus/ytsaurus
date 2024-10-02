#pragma once

#include "public.h"

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IAttributeProfiler
    : public TRefCounted
{
    using TValueTransform = std::function<TString(NYTree::INodePtr)>;

    virtual void Profile(TTransaction* transaction, const TObject* object) = 0;
    virtual void SetValueTransform(TValueTransform transform) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAttributeProfiler)

IAttributeProfilerPtr CreateAttributeProfiler(
    IObjectTypeHandler* typeHandler,
    const NYPath::TYPath& path,
    NClient::NProto::EAttributeSensorPolicy policy);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
