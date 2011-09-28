#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IYPathService
{
    DECLARE_ENUM(ECode,
        (Done)
        (Recurse)
        (Error)
    );

    template <class T>
    struct TResult
    {
        ECode Code;
        
        // Done
        T Value;

        // Recurse
        TIntrusiveConstPtr<INode> RecurseNode;
        TYPath RecursePath;
        
        // Error
        Stroka ErrorMessage;

        static TResult CreateDone(const T&value)
        {
            TResult result;
            result.Code = ECode::Done;
            result.Value = value;
            return result;
        }

        static TResult CreateRecurse(
            TIntrusiveConstPtr<INode> recurseNode,
            const TYPath& recursePath)
        {
            TResult result;
            result.Code = ECode::Recurse;
            result.RecurseNode = recurseNode;
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

    typedef TResult< TIntrusiveConstPtr<INode> > TNavigateResult;
    virtual TNavigateResult Navigate(const TYPath& path) const = 0;

    typedef TResult<TVoid> TGetResult;
    virtual TGetResult Get(const TYPath& path, IYsonConsumer* events) const = 0;

    typedef TResult<TVoid> TSetResult;
    virtual TSetResult Set(const TYPath& path, TYsonProducer::TPtr producer) = 0;

    typedef TResult<TVoid> TRemoveResult;
    virtual TRemoveResult Remove(const TYPath& path) = 0;
};

////////////////////////////////////////////////////////////////////////////////

void ChopYPathPrefix(
    const TYPath& path,
    Stroka* prefix,
    TYPath* tailPath);

INode::TConstPtr NavigateYPath(
    INode::TConstPtr root,
    const TYPath& path);

void GetYPath(
    INode::TConstPtr root,
    const TYPath& path,
    IYsonConsumer* events);

void SetYPath(
    INode::TConstPtr root,
    const TYPath& path,
    TYsonProducer::TPtr producer);

void RemoveYPath(
    INode::TConstPtr root,
    const TYPath& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
