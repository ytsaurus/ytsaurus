#pragma once

#include "id.h"
#include "object_proxy.h"
#include "object_ypath.pb.h"
#include "ypath.pb.h"

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

class TUntypedObjectProxyBase
    : public IObjectProxy
    , public NYTree::TYPathServiceBase
{
public:
    TUntypedObjectProxyBase(
        const TObjectId& id,
        const Stroka& loggingCategory = ObjectServerLogger.GetCategory());

    TObjectId GetId() const;

    virtual bool IsLogged(NRpc::IServiceContext* context) const;

protected:
    TObjectId Id;

    void DoInvoke(NRpc::IServiceContext* context);

    DECLARE_RPC_SERVICE_METHOD(NObjectServer::NProto, GetId);
    DECLARE_RPC_SERVICE_METHOD(NYTree::NProto, Get);
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TObjectProxyBase
    : public TUntypedObjectProxyBase
{
public:
    typedef typename NMetaState::TMetaStateMap<TObjectId, TObject> TMap;

    TObjectProxyBase(
        const TObjectId& id,
        TMap* map,
        const Stroka& loggingCategory = ObjectServerLogger.GetCategory())
        : TUntypedObjectProxyBase(id, loggingCategory)
        , Map(map)
    {
        YASSERT(map);
    }

protected:
    TMap* Map;

    const TObject& GetImpl() const
    {
        return Map->Get(Id);
    }

    TObject& GetImplForUpdate()
    {
        return Map->GetForUpdate(Id);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

