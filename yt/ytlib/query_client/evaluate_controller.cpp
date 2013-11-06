#include "evaluate_controller.h"

#include "private.h"
#include "helpers.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"

#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/row.h>
#include <ytlib/new_table_client/name_table.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <core/concurrency/fiber.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEvaluateController::TEvaluateController(
    IEvaluateCallbacks* callbacks,
    const TPlanFragment& fragment,
    TWriterPtr writer)
    : Callbacks_(callbacks)
    , Fragment_(fragment)
    , Writer_(std::move(writer))
    , Logger(QueryClientLogger)
{
    Logger.AddTag(Sprintf(
        "FragmendId: %s",
        ~ToString(Fragment_.Guid())));
}

TEvaluateController::~TEvaluateController()
{ }

TError TEvaluateController::Run()
{
    LOG_DEBUG("Evaluating plan fragment");

    switch (GetHead()->GetKind()) {
        case EOperatorKind::Union:
            return RunUnion();
        case EOperatorKind::Project:
            return RunProject();
        default:
            YUNIMPLEMENTED();
    }
}

TError TEvaluateController::RunUnion()
{
    LOG_DEBUG("Evaluating union fragment");

    auto* unionOp = GetHead()->As<TUnionOperator>();
    YCHECK(unionOp);

    LOG_DEBUG("Got %" PRISZT " sources", unionOp->Sources().size());

    // Prepare to write stuff.
    auto nameTable = New<TNameTable>();
    TTableSchema tableSchema;
    TKeyColumns keyColumns;
    bool didOpenWriter = false;

    std::vector<TRow> rows;
    rows.reserve(1000);

    for (const auto& source : unionOp->Sources()) {
        auto* scanOp = source->As<TScanOperator>();
        YCHECK(scanOp);
        
        tableSchema = GetTableSchemaFromDataSplit(scanOp->DataSplit());
        keyColumns = GetKeyColumnsFromDataSplit(scanOp->DataSplit());

        LOG_DEBUG("Opening reader");
        auto reader = GetCallbacks()->GetReader(scanOp->DataSplit());
        auto error = WaitFor(reader->Open(nameTable, tableSchema));
        RETURN_IF_ERROR(error);

        if (!didOpenWriter) {
            LOG_DEBUG("Opening writer");
            Writer_->Open(nameTable, tableSchema, keyColumns);
            didOpenWriter = true;
        } else {
            /*YCHECK(tableSchema == GetTableSchemaFromDataSplit(scanOp->DataSplit()));
            YCHECK(keyColumns == GetKeyColumnsFromDataSplit(scanOp->DataSplit()));*/
        }

        

        while (true) {
            bool hasData = reader->Read(&rows);
            for (auto row : rows) {
                for (int i = 0; i < row.GetValueCount(); ++i) {
                    Writer_->WriteValue(row[i]);
                }
                if (!Writer_->EndRow()) {
                    auto result = WaitFor(Writer_->GetReadyEvent());
                    RETURN_IF_ERROR(result);
                }
            }
            if (!hasData) {
                break;
            }
            if (rows.size() < rows.capacity()) {
                auto result = WaitFor(reader->GetReadyEvent());
                RETURN_IF_ERROR(result);
            }
            rows.clear();
        }
    }

    if (didOpenWriter) {
        return WaitFor(Writer_->AsyncClose());
    } else {
        return TError();
    }
}

TError TEvaluateController::RunProject()
{
    LOG_DEBUG("Evaluating project fragment");

    auto* projectOp = GetHead()->As<TProjectOperator>();
    YCHECK(projectOp);

    for (const auto& projection : projectOp->Projections()) {
        YCHECK(projection->GetKind() == EExpressionKind::Reference);
    }

    const TFilterOperator* filterOp = nullptr;
    const TScanOperator* scanOp = nullptr;

    {
        auto* currentOp = projectOp->GetSource();
        while (currentOp) {
            switch (currentOp->GetKind()) {
                case EOperatorKind::Filter:
                    filterOp = currentOp->As<TFilterOperator>();
                    currentOp = filterOp->GetSource();
                    break;
                case EOperatorKind::Scan:
                    scanOp = currentOp->As<TScanOperator>();
                    currentOp = nullptr;
                    break;
                default:
                    YUNIMPLEMENTED();
            }
        }
    }

    YCHECK(scanOp);

    auto nameTable = New<TNameTable>();
    auto keyColumns = InferKeyColumns(projectOp);
    auto writerSchema = InferTableSchema(projectOp);
    auto readerSchema = GetTableSchemaFromDataSplit(scanOp->DataSplit());

    LOG_DEBUG("Opening reader");
    auto reader = GetCallbacks()->GetReader(scanOp->DataSplit());
    auto error = WaitFor(reader->Open(nameTable, readerSchema));
    RETURN_IF_ERROR(error);

    LOG_DEBUG("Opening writer");
    Writer_->Open(nameTable, writerSchema, keyColumns);

    std::vector<TRow> rows;
    rows.reserve(1000);

    TSmallVector<int, TypicalProjectionCount> writerIndexToReaderIndex;
    TSmallVector<int, TypicalProjectionCount> writerIndexToNameIndex;
    writerIndexToReaderIndex.resize(projectOp->GetProjectionCount());
    writerIndexToNameIndex.resize(projectOp->GetProjectionCount());

    for (int i = 0; i < projectOp->GetProjectionCount(); ++i) {
        const auto& projection = projectOp->GetProjection(i);
        auto name = projection->As<TReferenceExpression>()->GetName();
        auto index = readerSchema.GetColumnIndex(readerSchema.GetColumnOrThrow(name));
        writerIndexToReaderIndex[i] = index;
        writerIndexToNameIndex[i] = nameTable->GetIndex(projection->InferName());
    }

    // Dumb way to do filter. :)
    int lhsReaderIndex = -1;
    i64 lhsValue = -1;
    i64 rhsValue = -1;
    EBinaryOp binaryOpOpcode = EBinaryOp::Equal;

    if (filterOp) {
        auto binaryOpExpr = filterOp->GetPredicate()->As<TBinaryOpExpression>();
        YCHECK(binaryOpExpr);
        auto lhsReferenceExpr = binaryOpExpr->GetLhs()->As<TReferenceExpression>();
        auto rhsIntegerValueExpr = binaryOpExpr->GetRhs()->As<TIntegerLiteralExpression>();
        YCHECK(lhsReferenceExpr);
        YCHECK(lhsReferenceExpr->Typecheck() == EColumnType::Integer);
        YCHECK(rhsIntegerValueExpr);

        binaryOpOpcode = binaryOpExpr->GetOpcode();
        lhsReaderIndex =
            readerSchema.GetColumnIndex(
                readerSchema.GetColumnOrThrow(
                    lhsReferenceExpr->GetName()));
        lhsValue = 0;
        rhsValue = rhsIntegerValueExpr->GetValue();
    }

    while (true) {
        bool hasData = reader->Read(&rows);
        for (auto row : rows) {
            bool predicate = true;
            if (filterOp) {
                lhsValue = row[lhsReaderIndex].Data.Integer;
                switch (binaryOpOpcode) {
                    case EBinaryOp::Less:
                        predicate = (lhsValue < rhsValue);
                        break;
                    case EBinaryOp::LessOrEqual:
                        predicate = (lhsValue <= rhsValue);
                        break;
                    case EBinaryOp::Equal:
                        predicate = (lhsValue == rhsValue);
                        break;
                    case EBinaryOp::NotEqual:
                        predicate = (lhsValue != rhsValue);
                        break;
                    case EBinaryOp::Greater:
                        predicate = (lhsValue > rhsValue);
                        break;
                    case EBinaryOp::GreaterOrEqual:
                        predicate = (lhsValue >= rhsValue);
                        break;
                }
            }
            if (!predicate) {
                continue;
            }
            for (int i = 0; i < projectOp->GetProjectionCount(); ++i) {
                TRowValue value = row[writerIndexToReaderIndex[i]];
                value.Index = writerIndexToNameIndex[i];
                Writer_->WriteValue(value);
            }
            if (!Writer_->EndRow()) {
                auto result = WaitFor(Writer_->GetReadyEvent());
                RETURN_IF_ERROR(result);
            }
        }
        if (!hasData) {
            break;
        }
        if (rows.size() < rows.capacity()) {
            auto result = WaitFor(reader->GetReadyEvent());
            RETURN_IF_ERROR(result);
        }
        rows.clear();
    }

    return WaitFor(Writer_->AsyncClose());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

