#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellBundleProxy
    : public NObjectServer::TNonversionedObjectProxyBase<TCellBundle>
{
public:
    TCellBundleProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        TCellBundle* cellBundle);

protected:
    using TBase = TNonversionedObjectProxyBase<TCellBundle>;

    virtual void ValidateRemoval() override;
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
