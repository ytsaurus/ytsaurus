#include "evaluate_controller.h"

#include "private.h"

#include "ast.h"
#include "ast_visitor.h"

#include "executor.h"

#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/name_table.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <core/concurrency/fiber.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NVersionedTableClient;
using namespace NConcurrency;

class TDumbReader
    : public IReader
{
public:
    TDumbReader(std::vector<IReaderPtr> readers)
        : Readers_(std::move(readers))
        , CurrentReader_(0)
    { }

    virtual TAsyncError Open(
        TNameTablePtr nameTable, 
        const NVersionedTableClient::NProto::TTableSchemaExt& schema,
        bool includeAllColumns = false,
        ERowsetType rowsetType = ERowsetType::Simple) override
    {
        for (auto reader : Readers_) {
            auto error = WaitFor(
                reader->Open(nameTable, schema, includeAllColumns, rowsetType));
            if (!error.IsOK()) {
                return MakeFuture(error);
            }
        }
        return MakeFuture(TError());
    }

    // Returns true while reading is in progress, false when reading is complete.
    // If rows->size() < rows->capacity(), wait for ready event before next call to #Read.
    // Can throw, e.g. if some values in chunk are incompatible with schema.
    // rows must be empty
    virtual bool Read(std::vector<TRow>* rows) override
    {
        if (CurrentReader_ >= Readers_.size()) {
            return false;
        }
        if (!Readers_[CurrentReader_]->Read(rows)) {
            CurrentReader_++;
        }
        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        if (CurrentReader_ >= Readers_.size()) {
            return MakeFuture(TError(42, "Reading complete!"));
        }
        return Readers_[CurrentReader_]->GetReadyEvent();
    }

private:
    std::vector<IReaderPtr> Readers_;
    int CurrentReader_;

};

TEvaluateController::TEvaluateController(
    IEvaluateCallbacks* callbacks,
    const TQueryFragment& fragment)
    : Callbacks_(callbacks)
    , Fragment_(fragment)
    , Logger(QueryClientLogger)
{
    Logger.AddTag(Sprintf(
        "FragmendId: %s",
        ~ToString(Fragment_.Guid())));
}

TEvaluateController::~TEvaluateController()
{ }

TError TEvaluateController::Run(TWriterPtr writer)
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
            writer->Open(nameTable, tableSchema, keyColumnsVector);
            openedWriter = true;
        }

        while (reader->Read(&rows)) {
            for (auto row : rows) {
                for (int i = 0; i < row.GetValueCount(); ++i) {
                    writer->WriteValue(row[i]);
                }
                if (!writer->EndRow()) {
                    auto result = WaitFor(writer->GetReadyEvent());
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

    auto error = WaitFor(writer->AsyncClose());
    if (!error.IsOK()) {
        return error;
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

