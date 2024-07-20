#pragma once

#include "dot.h"
#include "operations.h"
#include "tables.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IPlainGraphVisitor
{
public:
    virtual ~IPlainGraphVisitor() = default;

    virtual void OnTableNode(TYtGraphV2::TTableNode* /*tableNode*/)
    { }

    virtual void OnOperationNode(TYtGraphV2::TOperationNode* /*operationNode*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TDotVisitor
    : public IPlainGraphVisitor
{
public:
    void OnTableNode(TYtGraphV2::TTableNode* tableNode) override
    {
        if (tableNode->GetTableType() == ETableType::Output) {
            auto pathSchemaList = tableNode->VerifiedAsPtr<TOutputTableNode>()->GetPathSchemaList();
            for (const auto& [path, _] : pathSchemaList) {
                OnTable(path.Path_);
            }
        } else {
            auto name = OnTable(tableNode->GetPath().Path_);

            for (const auto& [consumer, _] : tableNode->InputFor) {
                DOTEdge(Stream_, name, DOTOperationName(GetOperationIndex(consumer)));
            }
        }
    }

    void OnOperationNode(TYtGraphV2::TOperationNode* operationNode) override
    {
        auto name = DOTOperationName(GetOperationIndex(operationNode));
        auto label = GetOperationLabel(operationNode);
        DOTOperation(Stream_, name, label);

        for (const auto& [_, output] : operationNode->OutputTables) {
            if (output->GetTableType() == ETableType::Output) {
                auto pathSchemaList = output->VerifiedAsPtr<TOutputTableNode>()->GetPathSchemaList();
                for (const auto& [path, _] : pathSchemaList) {
                    DOTEdge(Stream_, name, DOTTableName(GetTableIndex(path.Path_)));
                }
            } else {
                DOTEdge(Stream_, name, DOTTableName(GetTableIndex(output->GetPath().Path_)));
            }
        }
    }

    void Prologue()
    {
        Stream_.Clear();

        DOTPrologue(Stream_);
    }

    void Epilogue()
    {
        DOTEpilogue(Stream_);
    }

    TString GetResult() const
    {
        return Stream_.Str();
    }

private:
    TString OnTable(TString path)
    {
        auto name = DOTTableName(GetTableIndex(path));
        DOTTable(Stream_, name, path);

        return name;
    }

    TString GetPath(TYtGraphV2::TTableNode* table)
    {
        return table->GetPath().Path_;
    }

    TString GetOperationLabel(TYtGraphV2::TOperationNode* operationNode)
    {
        return operationNode->GetFirstName();
    }

    int GetTableIndex(const TString& tablePath)
    {
        if (!TableIndex_.contains(tablePath)) {
            TableIndex_[tablePath] = TableIndex_.size();
        }

        return TableIndex_[tablePath];
    }

    int GetOperationIndex(const TYtGraphV2::TOperationNode* operation)
    {
        if (!OperationIndex_.contains(operation)) {
            OperationIndex_[operation] = OperationIndex_.size();
        }

        return OperationIndex_[operation];
    }

private:
    TStringStream Stream_;
    THashMap<TString, int> TableIndex_;
    THashMap<const TYtGraphV2::TOperationNode*, int> OperationIndex_;
};

////////////////////////////////////////////////////////////////////////////////

void TraverseInTopologicalOrder(const TYtGraphV2::TPlainGraph& plainGraph, IPlainGraphVisitor* visitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
