#pragma once

#include "public.h"

#include "query_context.h"
#include "stubs.h"

#include <core/misc/property.h>
#include <core/misc/small_vector.h>

#include <util/string/escape.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TSourceLocation
{
    inline int GetOffset() const
    {
        return begin;
    }

    inline int GetLength() const
    {
        return end - begin;
    }

    // Naming is to confirm to Bison interface.
    int begin;
    int end;
};

// Instatiated in parser.yy.
extern const TSourceLocation NullSourceLocation;

struct IAstVisitor;

template <class TAstNode, int TypicalChildCount>
class TAstNodeBase
    : public TQueryContext::TTrackedObject
{
public:
    explicit TAstNodeBase(TQueryContext* context)
        : TTrackedObject(context)
        , Parent_(nullptr)
        , Children_()
    { }

    inline const SmallVectorImpl<TAstNode*>& Children() const
    {
        return Children_;
    }

    inline TAstNode* const* ChildBegin() const
    {
        return Children_.begin();
    }

    inline TAstNode* const* ChildEnd() const
    {
        return Children_.end();
    }

    inline TAstNode* Parent() const
    {
        return Parent_;
    }

    inline void AttachChild(TAstNode* node)
    {
        YCHECK(!node->Parent_);
        Children_.push_back(node);
        node->Parent_ = static_cast<TAstNode*>(this);
    }

    virtual bool Accept(IAstVisitor*) = 0;

protected:
    TAstNode* Parent_;
    TSmallVector<TAstNode*, TypicalChildCount> Children_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#define AST_H_
#include "expression.h"
#include "operator.h"
#undef AST_H_

