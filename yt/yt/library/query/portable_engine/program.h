#pragma once

#include "registry.h"

#include <yt/yt/client/table_client/unversioned_row.h>

#include <variant>
#include <vector>

namespace NYT::NQueryClient::NPortable {

////////////////////////////////////////////////////////////////////////////////

//! Evaluations may run concurrently with independent scratch and row buffers.
class TExpressionProgram
{
public:
    ssize_t GetScratchValueCount() const;

    //! Sorted unique schema column indices referenced by the compiled typed expression.
    const std::vector<int>& GetReferenceIds() const;

    //! Scratch is exclusive to this call and must not overlap inputRow or result.
    //! Result must be initialized; its Id and Flags are preserved, even when it aliases inputRow.
    //! String-like results remain valid until rowBuffer is cleared or destroyed.
    void Evaluate(
        TValue* result,
        TRange<TValue> inputRow,
        TMutableRange<TValue> scratch,
        const TRowBufferPtr& rowBuffer) const;

private:
    struct TLiteralNode
    {
        TOwningValue Value;
    };

    struct TReferenceNode
    {
        int ColumnIndex = 0;
    };

    struct TCallNode
    {
        TResolvedOperation Operation;
        std::vector<ssize_t> ArgumentIds;
    };

    using TNode = std::variant<TLiteralNode, TReferenceNode, TCallNode>;

    std::vector<TNode> Nodes_;
    std::vector<int> ReferenceIds_;
    ssize_t MaxArgumentCount_ = 0;

    TExpressionProgram() = default;

    friend class TProgramCompiler;
};

//! Compiles a typed expression; the result owns its literals and resolved operations.
TExpressionProgram CompileExpression(
    const TConstExpressionPtr& expression,
    const TTableSchema& schema,
    const TExpressionRegistry& registry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NPortable
