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
const int TypicalProjectionCount = 4;

template <class TPlanNode, class EKind>
class TPlanNodeBase
    : public TPlanContext::TTrackedObject
{
    EKind Kind_;

public:
    explicit TPlanNodeBase(TPlanContext* context, EKind kind)
        : TTrackedObject(context)
        , Kind_(kind)
    { }

    virtual ~TPlanNodeBase()
    { }

    inline EKind GetKind() const
    {
        return Kind_;
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

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#define PLAN_NODE_H_
#include "expression.h"
#include "operator.h"
#undef PLAN_NODE_H_

