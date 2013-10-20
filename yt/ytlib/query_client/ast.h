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
        , Children_()
        , Parent_(nullptr)
    { }

    inline const SmallVectorImpl<TAstNode*>& Children() const
    {
        return Children_;
    }

    inline TAstNode* Parent() const
    {
        return Parent_;
    }

    //! Adds a new child |node| to current node.
    inline void AddChild(TAstNode* node)
    {
        YASSERT(!node->Parent_);
        Children_.push_back(node);
        node->Parent_ = TypedThis();
    }

    //! Adds a new child |node| to current node.
    inline void AddChildNoCheck(TAstNode* node)
    {
        Children_.push_back(node);
        node->Parent_ = TypedThis();
    }

    //! Removes a child |node| from current node.
    inline void RemoveChild(TAstNode* node)
    {
        YASSERT(node->Parent_ == TypedThis());
        auto it = std::find(Children_.begin(), Children_.end(), node);
        YASSERT(it != Children_.end());
        Children_.erase(it);
        node->Parent_ = nullptr;
    }

    //! Swaps current node in-situ with |node| leaving current node dangling.
    inline void SwapWith(TAstNode* node)
    {
        YASSERT(!node->Parent_);
        YASSERT(node->Children_.empty());
        for (auto child : Children_) {
            child->Parent_ = nullptr;
            node->AddChild(child);
        }
        Children_.clear();
        if (Parent_) {
            auto it = Parent_->Children_.begin();
            auto jt = Parent_->Children_.end();
            if ((it = std::find(it, jt, TypedThis())) != jt) {
                *it = node;
            }
            node->Parent_ = Parent_;
            Parent_ = nullptr;
        }
    }

    //! Cuts the current node out from tree reattaching children to parent.
    inline void Cut()
    {
        for (auto child : Children_) {
            child->Parent_ = nullptr;
            if (Parent_) {
                Parent_->AddChild(child);
            }
        }
        Children_.clear();
        if (Parent_) {
            Parent_->RemoveChild(TypedThis());
            Parent_ = nullptr;
        }
    }

    virtual bool Accept(IAstVisitor*) = 0;

    virtual void Check() const
    {
        FOREACH (const auto& child, Children_) {
            YCHECK(this == child->Parent_);
        }
    }

protected:
    TSmallVector<TAstNode*, TypicalChildCount> Children_;
    TAstNode* Parent_;

private:
    TAstNode* TypedThis()
    {
        return static_cast<TAstNode*>(this);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#define AST_H_
#include "expression.h"
#include "operator.h"
#undef AST_H_

