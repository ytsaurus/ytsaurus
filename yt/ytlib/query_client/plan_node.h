#pragma once

#include "public.h"

#include "plan_context.h"
#include "source_location.h"

#include <ytlib/chunk_client/chunk_spec.h>

#include <core/misc/array_ref.h>
#include <core/misc/error.h>
#include <core/misc/property.h>
#include <core/misc/small_vector.h>

#include <util/string/escape.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

const int TypicalUnionArity = 3;
const int TypicalFunctionArity = 3;
const int TypicalNamedExpressionsCount = 4;

template <class TPlanNode, class EKind>
class TPlanNodeBase;

template <class TPlanNode>
class TPlanNodePtr;

template <class TPlanNode, class EKind>
class TPlanNodeBase
    : public TPlanContext::TTrackedObject
{
public:
    explicit TPlanNodeBase(TPlanContext* context, EKind kind)
        : TTrackedObject(context)
        , Kind_(kind)
        , RefCount_(1)
    { }

    virtual ~TPlanNodeBase()
    {
        // XXX(sandello): This is a WIP.
        // YCHECK(RefCount_ == 0);
    }

    int Ref() const
    {
        return ++RefCount_;
    }

    int Unref() const
    {
        return --RefCount_;
    }

    inline EKind GetKind() const
    {
        return Kind_;
    }

    inline int GetRefCount() const
    {
        return RefCount_;
    }

    TPlanNode* Clone(TPlanContext* context) const
    {
        return static_cast<const TPlanNode*>(this)->CloneImpl(context);
    }

    template <class TDerivedPlanNode>
    inline bool IsA(
        typename std::enable_if<
            std::is_base_of<TPlanNode, TDerivedPlanNode>::value,
            int
        >::type = 0) const
    {
        return TDerivedPlanNode::IsClassOf(static_cast<const TPlanNode*>(this));
    }

    template <class TDerivedPlanNode>
    inline const TDerivedPlanNode* As(
        typename std::enable_if<
            std::is_base_of<TPlanNode, TDerivedPlanNode>::value,
            int
        >::type = 0) const
    {
        if (IsA<TDerivedPlanNode>()) {
            return static_cast<const TDerivedPlanNode*>(this);
        } else {
            return nullptr;
        }
    }

    template <class TDerivedPlanNode>
    inline TDerivedPlanNode* As(
        typename std::enable_if<
            std::is_base_of<TPlanNode, TDerivedPlanNode>::value,
            int
        >::type = 0)
    {
        if (IsA<TDerivedPlanNode>()) {
            return static_cast<TDerivedPlanNode*>(this);
        } else {
            return nullptr;
        }
    }

    // TODO(sandello): Implement CoW strategy here to avoid subtle issues.
    template <class TDerivedPlanNode>
    inline TDerivedPlanNode* AsMutable(
        typename std::enable_if<
            std::is_base_of<TPlanNode, TDerivedPlanNode>::value,
            int
        >::type = 0) const
    {
        return const_cast<TDerivedPlanNode*>(As<TDerivedPlanNode>());
    }

    virtual TArrayRef<const TPlanNode*> Children() const = 0;

private:
    EKind Kind_;
    mutable int RefCount_;

};

//! A smart pointer which enables copy-on-write semantics for the stored value.
//! This is not a generic code since the pointer should be aware about cloning
//! strategy which is non-trivial in case of polymorphic objects.
template <class TPlanNode>
class TPlanNodePtr
{
public:
    TPlanNodePtr() noexcept
        : Node_(nullptr)
    { }

    TPlanNodePtr(TPlanNode* p, bool addReference = true) noexcept
        : Node_(p)
    {
        if (Node_ && addReference) {
            Node_->Ref();
        }
    }

    TPlanNodePtr(const TPlanNodePtr& other) noexcept
        : Node_(other.Node_)
    {
        Node_->Ref();
    }

    TPlanNodePtr(TPlanNodePtr&& other) noexcept
        : Node_(other.Node_)
    {
        other.Node_ = nullptr;
    }

    ~TPlanNodePtr()
    {
        if (Node_) {
            Node_->Unref();
        }
    }

    TPlanNodePtr& operator=(const TPlanNodePtr& other) noexcept
    {
        TPlanNodePtr(other).Swap(*this);
        return *this;
    }

    TPlanNodePtr& operator=(TPlanNodePtr&& other) noexcept
    {
        TPlanNodePtr(std::move(other)).Swap(*this);
        return *this;
    }

    explicit operator bool() const
    {
        return Node_;
    }

    void Reset() noexcept
    {
        TPlanNodePtr().Swap(*this);
    }

    void Reset(TPlanNode* p) noexcept
    {
        TPlanNodePtr(p).Swap(*this);
    }

    void Swap(TPlanNodePtr& other) noexcept
    {
        std::swap(Node_, other.Node_);
    }

    const TPlanNode* Get() const noexcept
    {
        return Node_;
    }

    TPlanNode* GetMutable()
    {
        if (!Node_ || Node_->GetRefCount() == 1) {
            return Node_;
        }

        TPlanNodePtr(Node_->Clone(), false).Swap(*this);
        YASSERT(Node_->GetRefCount() == 1);
        return Node_;
    }

    const TPlanNode& operator*() const noexcept
    {
        YASSERT(Node_);
        return *Node_;
    }

    const TPlanNode* operator->() const noexcept
    {
        YASSERT(Node_);
        return Node_;
    }

private:
    TPlanNode* Node_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#define PLAN_NODE_H_
#include "expression.h"
#include "operator.h"
#undef PLAN_NODE_H_

