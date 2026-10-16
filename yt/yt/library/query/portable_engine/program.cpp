#include "program.h"

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_visitors.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/variant.h>

#include <util/generic/algorithm.h>

#include <algorithm>
#include <iterator>
#include <utility>

namespace NYT::NQueryClient::NPortable {

////////////////////////////////////////////////////////////////////////////////

class TProgramCompiler
    : public TBaseVisitor<ssize_t, TProgramCompiler>
{
public:
    TProgramCompiler(const TTableSchema& schema, const TExpressionRegistry& registry)
        : Schema_(schema)
        , Registry_(registry)
    { }

    TExpressionProgram Run(const TConstExpressionPtr& expression)
    {
        Visit(expression);

        SortUnique(Program_.ReferenceIds_);

        return std::move(Program_);
    }

    ssize_t OnLiteral(const TLiteralExpression* expression)
    {
        return AddNode(TExpressionProgram::TLiteralNode{expression->Value});
    }

    ssize_t OnReference(const TReferenceExpression* expression)
    {
        int columnIndex;
        try {
            columnIndex = Schema_.GetColumnIndexOrThrow(expression->ColumnName);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Cannot compile portable reference %Qv", expression->ColumnName)
                .With("expression_path", Path_)
                .With("expression_kind", EExpressionKind::Reference)
                .With("result_type", expression->GetWireType())
                .With(ex);
        }

        Program_.ReferenceIds_.push_back(columnIndex);
        return AddNode(TExpressionProgram::TReferenceNode{columnIndex});
    }

    ssize_t OnFunction(const TFunctionExpression* expression)
    {
        std::vector<EValueType> argumentTypes;
        argumentTypes.reserve(expression->Arguments.size());
        for (const auto& argument : expression->Arguments) {
            argumentTypes.push_back(argument->GetWireType());
        }

        auto operation = GetResolutionResultOrThrow(
            Registry_.FindFunction(expression->FunctionName, argumentTypes, expression->GetWireType()),
            EExpressionKind::Function,
            expression->FunctionName,
            argumentTypes,
            expression->GetWireType());

        std::vector<ssize_t> argumentIds;
        argumentIds.reserve(expression->Arguments.size());
        for (ssize_t index = 0; index < std::ssize(expression->Arguments); ++index) {
            argumentIds.push_back(VisitChild(expression->Arguments[index], Format(".arguments[%v]", index)));
        }

        return AddCall(std::move(operation), std::move(argumentIds));
    }

    ssize_t OnUnary(const TUnaryOpExpression* expression)
    {
        auto operation = GetResolutionResultOrThrow(
            Registry_.FindUnary(expression->Opcode, expression->Operand->GetWireType(), expression->GetWireType()),
            EExpressionKind::UnaryOp,
            Format("%lv", expression->Opcode),
            {expression->Operand->GetWireType()},
            expression->GetWireType());
        ssize_t operandId = VisitChild(expression->Operand, ".operand");
        return AddCall(std::move(operation), {operandId});
    }

    ssize_t OnBinary(const TBinaryOpExpression* expression)
    {
        auto operation = GetResolutionResultOrThrow(
            Registry_.FindBinary(
                expression->Opcode,
                expression->Lhs->GetWireType(),
                expression->Rhs->GetWireType(),
                expression->GetWireType()),
            EExpressionKind::BinaryOp,
            Format("%lv", expression->Opcode),
            {expression->Lhs->GetWireType(), expression->Rhs->GetWireType()},
            expression->GetWireType());
        ssize_t lhsId = VisitChild(expression->Lhs, ".lhs");
        ssize_t rhsId = VisitChild(expression->Rhs, ".rhs");
        return AddCall(std::move(operation), {lhsId, rhsId});
    }

    ssize_t OnIn(const TInExpression* expression)
    {
        return RejectNode(EExpressionKind::In, expression);
    }

    ssize_t OnBetween(const TBetweenExpression* expression)
    {
        return RejectNode(EExpressionKind::Between, expression);
    }

    ssize_t OnTransform(const TTransformExpression* expression)
    {
        return RejectNode(EExpressionKind::Transform, expression);
    }

    ssize_t OnCase(const TCaseExpression* expression)
    {
        return RejectNode(EExpressionKind::Case, expression);
    }

    ssize_t OnLike(const TLikeExpression* expression)
    {
        return RejectNode(EExpressionKind::Like, expression);
    }

    ssize_t OnCompositeMemberAccessor(const TCompositeMemberAccessorExpression* expression)
    {
        return RejectNode(EExpressionKind::CompositeMemberAccessor, expression);
    }

    ssize_t OnSubquery(const TSubqueryExpression* expression)
    {
        return RejectNode(EExpressionKind::Subquery, expression);
    }

private:
    const TTableSchema& Schema_;
    const TExpressionRegistry& Registry_;

    TExpressionProgram Program_;
    std::string Path_ = "root";

    ssize_t AddNode(TExpressionProgram::TNode node)
    {
        ssize_t nodeId = std::ssize(Program_.Nodes_);
        Program_.Nodes_.push_back(std::move(node));
        return nodeId;
    }

    ssize_t AddCall(TResolvedOperation operation, std::vector<ssize_t> argumentIds)
    {
        Program_.MaxArgumentCount_ = std::max(Program_.MaxArgumentCount_, std::ssize(argumentIds));
        return AddNode(TExpressionProgram::TCallNode{
            .Operation = std::move(operation),
            .ArgumentIds = std::move(argumentIds),
        });
    }

    ssize_t VisitChild(const TConstExpressionPtr& expression, const std::string& suffix)
    {
        auto parentPath = std::exchange(Path_, Path_ + suffix);
        ssize_t nodeId = Visit(expression);
        Path_ = std::move(parentPath);
        return nodeId;
    }

    TResolvedOperation GetResolutionResultOrThrow(
        std::optional<TResolvedOperation> operation,
        EExpressionKind kind,
        const std::string& name,
        TRange<EValueType> argumentTypes,
        EValueType resultType) const
    {
        if (!operation) {
            auto argumentTypeList = argumentTypes.ToVector();
            THROW_ERROR_EXCEPTION("Unsupported portable %Qlv %Qv with signature %lv -> %Qlv",
                kind,
                name,
                argumentTypeList,
                resultType)
                .With("expression_path", Path_)
                .With("expression_kind", kind)
                .With("operation", name)
                .With("argument_types", argumentTypeList)
                .With("result_type", resultType);
        }

        return std::move(*operation);
    }

    [[noreturn]] ssize_t RejectNode(EExpressionKind kind, const TExpression* expression) const
    {
        THROW_ERROR_EXCEPTION("Unsupported portable expression kind %Qlv", kind)
            .With("expression_path", Path_)
            .With("expression_kind", kind)
            .With("result_type", expression->GetWireType());
    }
};

////////////////////////////////////////////////////////////////////////////////

ssize_t TExpressionProgram::GetScratchValueCount() const
{
    return std::ssize(Nodes_) + MaxArgumentCount_;
}

const std::vector<int>& TExpressionProgram::GetReferenceIds() const
{
    return ReferenceIds_;
}

void TExpressionProgram::Evaluate(
    TValue* result,
    TRange<TValue> inputRow,
    TMutableRange<TValue> scratch,
    const TRowBufferPtr& rowBuffer) const
{
    YT_VERIFY(result);
    YT_VERIFY(!Nodes_.empty());
    YT_VERIFY(std::ssize(scratch) >= GetScratchValueCount());
    YT_VERIFY(ReferenceIds_.empty() || std::ssize(inputRow) > ReferenceIds_.back());

    auto arguments = scratch.Slice(Nodes_.size(), GetScratchValueCount());
    for (ssize_t nodeId = 0; nodeId < std::ssize(Nodes_); ++nodeId) {
        Visit(
            Nodes_[nodeId],
            [&] (const TLiteralNode& node) {
                scratch[nodeId] = node.Value;
            },
            [&] (const TReferenceNode& node) {
                scratch[nodeId] = inputRow[node.ColumnIndex];
            },
            [&] (const TCallNode& node) {
                for (ssize_t index = 0; index < std::ssize(node.ArgumentIds); ++index) {
                    arguments[index] = scratch[node.ArgumentIds[index]];
                }
                InvokeOperation(
                    node.Operation,
                    &scratch[nodeId],
                    arguments.Slice(/*startOffset*/ 0, node.ArgumentIds.size()),
                    rowBuffer);
            });
    }

    auto rootValue = scratch[Nodes_.size() - 1];
    if (!std::holds_alternative<TCallNode>(Nodes_.back())) {
        rootValue = rowBuffer->CaptureValue(rootValue);
    }
    rootValue.Id = result->Id;
    rootValue.Flags = result->Flags;
    *result = rootValue;
}

TExpressionProgram CompileExpression(
    const TConstExpressionPtr& expression,
    const TTableSchema& schema,
    const TExpressionRegistry& registry)
{
    return TProgramCompiler(schema, registry).Run(expression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NPortable
