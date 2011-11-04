#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IYPathService
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IYPathService> TPtr;

    static IYPathService::TPtr FromNode(INode* node);
    static IYPathService::TPtr FromProducer(TYsonProducer* producer);

    DECLARE_ENUM(ECode,
        (Undefined)
        (Done)
        (Recurse)
    );

    template <class T>
    struct TResult
    {
        TResult()
            : Code(ECode::Undefined)
        { }

        template <class TOther>
        TResult(const TResult<TOther>& other)
        {
            YASSERT(other.Code == ECode::Recurse);
            Code = ECode::Recurse;
            RecurseService = other.RecurseService;
            RecursePath = other.RecursePath;
        }

        ECode Code;
        
        // Done
        T Value;

        // Recurse
        IYPathService::TPtr RecurseService;
        TYPath RecursePath;
        
        static TResult CreateDone(const T& value = T())
        {
            TResult result;
            result.Code = ECode::Done;
            result.Value = value;
            return result;
        }

        static TResult CreateRecurse(
            IYPathService::TPtr recurseService,
            TYPath recursePath)
        {
            TResult result;
            result.Code = ECode::Recurse;
            result.RecurseService = recurseService;
            result.RecursePath = recursePath;
            return result;
        }
    };

    typedef TResult<INode::TPtr> TNavigateResult;
    virtual TNavigateResult Navigate(TYPath path) = 0;

    typedef TResult<TVoid> TGetResult;
    virtual TGetResult Get(TYPath path, IYsonConsumer* consumer) = 0;

    typedef TResult<INode::TPtr> TSetResult;
    virtual TSetResult Set(TYPath path, TYsonProducer::TPtr producer) = 0;

    typedef TResult<TVoid> TRemoveResult;
    virtual TRemoveResult Remove(TYPath path) = 0;

    typedef TResult<TVoid> TLockResult;
    virtual TLockResult Lock(TYPath path) = 0;
};

////////////////////////////////////////////////////////////////////////////////

void ChopYPathPrefix(
    TYPath path,
    Stroka* prefix,
    TYPath* tailPath);

INode::TPtr NavigateYPath(
    IYPathService::TPtr rootService,
    TYPath path);

void GetYPath(
    IYPathService::TPtr rootService,
    TYPath path,
    IYsonConsumer* consumer);

INode::TPtr SetYPath(
    IYPathService::TPtr rootService,
    TYPath path,
    TYsonProducer::TPtr producer);

void RemoveYPath(
    IYPathService::TPtr rootService,
    TYPath path);

void LockYPath(
    IYPathService::TPtr rootService,
    TYPath path);

////////////////////////////////////////////////////////////////////////////////

class TYTreeException : public yexception
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
