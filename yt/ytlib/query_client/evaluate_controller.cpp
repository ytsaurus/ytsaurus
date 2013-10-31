#include "evaluate_controller.h"

#include "private.h"
#include "helpers.h"

#include "ast.h"
#include "ast_visitor.h"

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
    using namespace NVersionedTableClient;

    LOG_DEBUG("Evaluating fragment");

    std::vector<const TScanOperator*> scanOps;
    Visit(GetHead(), [&] (const TOperator* op) {
        if (auto* typedOp = op->As<TScanOperator>()) {
            scanOps.push_back(typedOp);
        }
    });

    LOG_DEBUG("Got %" PRISZT " scan operators in fragment", scanOps.size());

    auto nameTable = New<TNameTable>();
    TTableSchema tableSchema;
    TKeyColumns keyColumns;
    bool didOpenWriter = false;

    std::vector<TRow> rows;
    rows.reserve(1000);

    for (const auto& scanOp : scanOps) {
        LOG_DEBUG("Opening reader");
        auto reader = GetCallbacks()->GetReader(scanOp->DataSplit());
        auto error = WaitFor(reader->Open(nameTable, tableSchema));
        RETURN_IF_ERROR(error);

        if (!didOpenWriter) {
            LOG_DEBUG("Opening writer");
            tableSchema = GetTableSchemaFromDataSplit(scanOp->DataSplit());
            keyColumns = GetKeyColumnsFromDataSplit(scanOp->DataSplit());
            Writer_->Open(nameTable, tableSchema, keyColumns);
            didOpenWriter = true;
        }

        printf("about to read something from %s\n",
            ~ToString(GetObjectIdFromDataSplit(scanOp->DataSplit())));

        while (true) {
            bool hasData = reader->Read(&rows);
            printf("new batch!\n");
            for (auto row : rows) {
                printf("new row!\n");
                for (int i = 0; i < row.GetValueCount(); ++i) {
                    printf("new value!\n");
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

    auto error = WaitFor(Writer_->AsyncClose());
    if (!error.IsOK()) {
        return error;
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

