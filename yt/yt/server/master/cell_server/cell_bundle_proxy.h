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
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

protected:
    using TBase = TNonversionedObjectProxyBase<TCellBundle>;

    void ValidateRemoval() override;
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;

private:
    void DoSerializeAccountViolatedResourceLimits(
        NSecurityServer::TAccount* account,
        NChunkServer::TMedium* medium,
        NYson::IYsonConsumer* consumer) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
