#pragma once

#include "cg_fragment_compiler.h"

#include "key_trie.h"
#include "plan_fragment.h"

#include <core/codegen/module.h>

#include <core/misc/variant.h>
#include <core/misc/ref_counted.h>

#include <util/generic/stroka.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IFunctionDescriptor
    : public TRefCounted
{
    virtual ~IFunctionDescriptor();

    virtual Stroka GetName() const = 0;

    virtual EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const TStringBuf& source) const = 0;

    virtual TCodegenExpression MakeCodegenExpr(
        std::vector<TCodegenExpression> codegenArgs,
        EValueType type,
        const Stroka& name) const = 0;

    virtual TKeyTrieNode ExtractKeyRange(
        const TIntrusivePtr<const TFunctionExpression>& expr,
        const TKeyColumns& keyColumns,
        TRowBuffer* rowBuffer) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IFunctionDescriptor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
