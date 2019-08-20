#pragma once

#include "public.h"
#include "cell_base.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/public.h>
#include <yt/server/master/object_server/object_detail.h>

#include <yt/server/lib/misc/public.h>

#include <yt/core/ytree/ypath_detail.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellProxyBase
    : public NObjectServer::TNonversionedObjectProxyBase<TCellBase>
{
public:
    TCellProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        TCellBase* cell);

protected:
    typedef NObjectServer::TNonversionedObjectProxyBase<TCellBase> TBase;

    virtual void ValidateRemoval() override;
    virtual void RemoveSelf(TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override;
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
