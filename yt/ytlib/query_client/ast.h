#pragma once

#include "public.h"

#include "query_context.h"

#include <ytlib/chunk_client/chunk_spec.h>

#include <core/misc/array_ref.h>
#include <core/misc/error.h>
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

    Stroka ToString() const
    {
        return Sprintf("%d:%d", begin, end);
    }

    // Naming is to confirm to Bison interface.
    int begin;
    int end;
};

// Instatiated in parser.yy.
extern const TSourceLocation NullSourceLocation;

struct IAstVisitor;

const int TypicalUnionArity = 3;
const int TypicalFunctionArity = 3;
const int TypicalProjectionCount = 4;

template <class TAstNode, class EKind>
class TAstNodeBase
    : public TQueryContext::TTrackedObject
{
    EKind Kind_;

public:
    explicit TAstNodeBase(TQueryContext* context, EKind kind)
        : TTrackedObject(context)
        , Kind_(kind)
    { }

    virtual ~TAstNodeBase()
    { }

    inline EKind GetKind() const
    {
        return Kind_;
    }

    template <class TDerivedAstNode>
    inline bool IsA(
        typename std::enable_if<
            std::is_base_of<TAstNode, TDerivedAstNode>::value,
            int
        >::type = 0) const
    {
        return TDerivedAstNode::IsClassOf(static_cast<const TAstNode*>(this));
    }

    template <class TDerivedAstNode>
    inline const TDerivedAstNode* As(
        typename std::enable_if<
            std::is_base_of<TAstNode, TDerivedAstNode>::value,
            int
        >::type = 0) const
    {
        if (IsA<TDerivedAstNode>()) {
            return static_cast<const TDerivedAstNode*>(this);
        } else {
            return nullptr;
        }
    }

    template <class TDerivedAstNode>
    inline TDerivedAstNode* AsMutable(
        typename std::enable_if<
            std::is_base_of<TAstNode, TDerivedAstNode>::value,
            int
        >::type = 0) const
    {
        return const_cast<TDerivedAstNode*>(As<TDerivedAstNode>());
    }

    inline TAstNode* Parent() const
    {
        YUNREACHABLE();
    }

    virtual TArrayRef<const TAstNode*> Children() const = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#define AST_H_
#include "expression.h"
#include "operator.h"
#undef AST_H_

