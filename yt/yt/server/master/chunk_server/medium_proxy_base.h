#include "public.h"

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/ytree/static_service_dispatcher.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TMediumProxyBase
    : public NObjectServer::TNonversionedObjectProxyBase<TMedium>
    , public virtual NYTree::TStaticServiceDispatcher
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

protected:
    using TBase = NObjectServer::TNonversionedObjectProxyBase<TMedium>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
