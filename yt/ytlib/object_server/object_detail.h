#pragma once

#include "id.h"
#include "object_ypath.pb.h"

#include <ytlib/misc/property.h>
#include <ytlib/meta_state/map.h>
#include <ytlib/ytree/ypath_detail.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TObjectBase
{
public:
    TObjectBase();

    //! Increments the object's reference counter.
    /*!
     *  \returns the incremented counter.
     */
    i32 RefObject();

    //! Decrements the object's reference counter.
    /*!
     *  \note
     *  Objects do not self-destruct, it's callers responsibility to check
     *  if the counter reaches zero.
     *  
     *  \returns the decremented counter.
     */
    i32 UnrefObject();

    //! Returns the current reference counter.
    i32 GetObjectRefCounter() const;

protected:
    TObjectBase(const TObjectBase& other);

    i32 RefCounter;

};

////////////////////////////////////////////////////////////////////////////////

class TObjectWithIdBase
    : public TObjectBase
{
    DEFINE_BYVAL_RO_PROPERTY(TObjectId, Id);

public:
    TObjectWithIdBase();
    TObjectWithIdBase(const TObjectId& id);
    TObjectWithIdBase(const TObjectWithIdBase& other);

};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectProxyBase
    : public IObjectProxy
    , public NYTree::TYPathServiceBase
{
public:
    typedef typename NMetaState::TMetaStateMap<TObjectId, TObject> TMap;

    TObjectProxyBase(const TObjectId& id, TMap* map)
        : Id(id)
        , Map(map)
    {
        YASSERT(map);
    }

    TObjectId GetId() const
    {
        return Id;
    }

    virtual bool IsLogged(NRpc::IServiceContext* context) const
    {
        UNUSED(context);
        return false;
    }

protected:
    TObjectId Id;
    TMap* Map;

    const TObject& GetImpl() const
    {
        return map->Get(Id);
    }

    TObject& GetImplForUpdate()
    {
        return Map->GetForUpdate(Id);
    }

    void DoInvoke(NRpc::IServiceContext* context)
    {
        Stroka verb = context->GetVerb();
        if (verb == "GetId") {
            GetIdThunk(context);
        } else {
            NYTree::TYPathServiceBase::DoInvoke(context);
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, GetId)
    {
        UNUSED(request);

        response->set_id(Id.ToProto());
        context->Reply();
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

