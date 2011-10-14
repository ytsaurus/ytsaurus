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

    DECLARE_ENUM(ECode,
        (Undefined)
        (Done)
        (Recurse)
        (Error)
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
            Code = other.Code;
            switch (other.Code) {
                case ECode::Recurse:
                    RecurseService = other.RecurseService;
                    RecursePath = other.RecursePath;
                    break;

                case ECode::Error:
                    ErrorMessage = other.ErrorMessage;
                    break;

                default:
                    YUNREACHABLE();
            }
        }

        ECode Code;
        
        // Done
        T Value;

        // Recurse
        IYPathService::TPtr RecurseService;
        TYPath RecursePath;
        
        // Error
        Stroka ErrorMessage;

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

        static TResult CreateError(Stroka errorMessage)
        {
            TResult result;
            result.Code = ECode::Error;
            result.ErrorMessage = errorMessage;
            return result;
        }
    };

    typedef TResult<INode::TPtr> TNavigateResult;
    virtual TNavigateResult Navigate(TYPath path) = 0;

    typedef TResult<TVoid> TGetResult;
    virtual TGetResult Get(TYPath path, IYsonConsumer* events) = 0;

    typedef TResult<TVoid> TSetResult;
    virtual TSetResult Set(TYPath path, TYsonProducer::TPtr producer) = 0;

    typedef TResult<TVoid> TRemoveResult;
    virtual TRemoveResult Remove(TYPath path) = 0;

    typedef TResult<TVoid> TLockResult;
    virtual TLockResult Lock(TYPath path) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IYPathService::TPtr AsYPath(INode::TPtr node);

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

void SetYPath(
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

class TYPathException : public yexception
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
