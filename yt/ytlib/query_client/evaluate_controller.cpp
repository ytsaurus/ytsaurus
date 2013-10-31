#include "evaluate_controller.h"

#include "private.h"

#include "ast.h"
#include "ast_visitor.h"

#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
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
    const TQueryFragment& fragment,
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
    LOG_DEBUG("Evaluating fragment");

    std::vector<const TScanOperator*> scanOps;
    Visit(GetHead(), [&] (const TOperator* op) {
        if (auto* typedOp = op->As<TScanOperator>()) {
            scanOps.push_back(typedOp);
        }
    });

    LOG_DEBUG("Got %" PRISZT " scan operators in fragment", scanOps.size());

    auto nameTable = New<TNameTable>();
    bool openedWriter = false;

    std::vector<NVersionedTableClient::TRow> rows;
    rows.reserve(1000);
    for (const auto& scanOp : scanOps) {
        auto reader = GetCallbacks()->GetReader(scanOp->DataSplit());
        auto tableSchema = GetProtoExtension<NVersionedTableClient::NProto::TTableSchemaExt>(scanOp->DataSplit().extensions());
        auto keyColumns = GetProtoExtension<NTableClient::NProto::TKeyColumnsExt>(scanOp->DataSplit().extensions());

        LOG_DEBUG("Opening reader");
        auto error = WaitFor(reader->Open(nameTable, tableSchema));
        if (!error.IsOK()) {
            return error;
        }

        if (!openedWriter) {
            LOG_DEBUG("Opening writer");
            NVersionedTableClient::TKeyColumns keyColumnsVector;
            for (int i = 0; i < keyColumns.values_size(); ++i) {
                keyColumnsVector.push_back(keyColumns.values(i));
            }
            Writer_->Open(nameTable, tableSchema, keyColumnsVector);
            openedWriter = true;
        }

        while (reader->Read(&rows)) {
            for (auto row : rows) {
                for (int i = 0; i < row.GetValueCount(); ++i) {
                    Writer_->WriteValue(row[i]);
                }
                if (!Writer_->EndRow()) {
                    auto result = WaitFor(Writer_->GetReadyEvent());
                    if (!result.IsOK()) {
                        return result;
                    }
                }
            }
            if (rows.size() < rows.capacity()) {
                auto result = WaitFor(reader->GetReadyEvent());
                if (!result.IsOK()) {
                    return result;
                }
            }
        }
    }

    auto error = WaitFor(Writer_->AsyncClose());
    if (!error.IsOK()) {
        return error;
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

