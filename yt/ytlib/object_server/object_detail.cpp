#include "stdafx.h"
#include "object_detail.h"

#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NObjectServer {

using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TObjectBase::TObjectBase()
    : RefCounter(0)
{ }

TObjectBase::TObjectBase(const TObjectBase& other)
    : RefCounter(other.RefCounter)
{ }

i32 TObjectBase::RefObject()
{
    return ++RefCounter;
}

i32 TObjectBase::UnrefObject()
{
    YASSERT(RefCounter > 0);
    return --RefCounter;
}

i32 TObjectBase::GetObjectRefCounter() const
{
    return RefCounter;
}

////////////////////////////////////////////////////////////////////////////////

TObjectWithIdBase::TObjectWithIdBase()
{ }

TObjectWithIdBase::TObjectWithIdBase(const TObjectId& id)
    : Id_(id)
{ }

TObjectWithIdBase::TObjectWithIdBase(const TObjectWithIdBase& other)
    : TObjectBase(other)
    , Id_(other.Id_)
{ }

////////////////////////////////////////////////////////////////////////////////

TUntypedObjectProxyBase::TUntypedObjectProxyBase(
    const TObjectId& id,
    const Stroka& loggingCategory)
    : TYPathServiceBase(loggingCategory)
    , Id(id)
{ }

TObjectId TUntypedObjectProxyBase::GetId() const
{
    return Id;
}

bool TUntypedObjectProxyBase::IsLogged(IServiceContext* context) const
{
    UNUSED(context);
    return false;
}

void TUntypedObjectProxyBase::DoInvoke( NRpc::IServiceContext* context )
{
    DISPATCH_YPATH_SERVICE_METHOD(GetId);
    DISPATCH_YPATH_SERVICE_METHOD(Get);
    NYTree::TYPathServiceBase::DoInvoke(context);
}

DEFINE_RPC_SERVICE_METHOD(TUntypedObjectProxyBase, GetId)
{
    UNUSED(request);

    response->set_id(Id.ToProto());
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TUntypedObjectProxyBase, Get)
{
    UNUSED(request);

    auto path = context->GetPath();

    if (NYTree::IsFinalYPath(path)) {
        response->set_value(NYTree::BuildYsonFluently().Entity());
        context->Reply();
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

