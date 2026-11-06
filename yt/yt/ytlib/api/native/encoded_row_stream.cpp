#include "encoded_row_stream.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/api/formatted_table_reader.h>
#include <yt/yt/client/api/row_batch_reader.h>

#include <yt/yt/core/actions/cancelable_context.h>
#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/async_stream_helpers.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TEncodedRowStream
    : public IFormattedTableReader
{
public:
    TEncodedRowStream(
        IRowBatchReaderPtr batchReader,
        TNameTablePtr nameTable,
        TFormat format,
        TTableSchemaPtr tableSchema,
        std::optional<std::vector<std::string>> columns,
        TControlAttributesConfigPtr controlAttributesConfig)
        : OutputStream_(Data_)
        , AsyncOutputStream_(CreateAsyncAdapter(&OutputStream_))
        , FormatWriter_(CreateStaticTableWriterForFormat(
            format,
            std::move(nameTable),
            {tableSchema},
            {std::move(columns)},
            AsyncOutputStream_,
            /*enableContextSaving*/ false,
            controlAttributesConfig,
            /*keyColumnCount*/ 0))
        , RowReader_(std::move(batchReader))
    { }

    TFuture<TSharedRef> Read() override
    {
        auto promise = NewPromise<TSharedRef>();
        auto context = New<TCancelableContext>();
        promise.OnCanceled(BIND(&TCancelableContext::Cancel, context));
        ContinueRead(promise, context, TError());
        return promise.ToFuture();
    }

private:
    void ContinueRead(
        const TPromise<TSharedRef>& promise,
        const TCancelableContextPtr& context,
        const TError& error)
    {
        if (!error.IsOK()) {
            promise.TrySet(error);
            return;
        }

        try {
            while (true) {
                if (context->IsCanceled()) {
                    promise.TrySet(
                        TError(NYT::EErrorCode::Canceled, "Read canceled")
                            << context->GetCancelationError());
                    return;
                }

                auto batch = RowReader_->Read();
                if (!batch) {
                    promise.TrySet(TSharedRef());
                    return;
                }

                if (!batch->IsEmpty()) {
                    Data_.clear();
                    FormatWriter_->WriteBatch(batch);
                    promise.SetFrom(FormatWriter_->Flush().Apply(
                        BIND([this, this_ = MakeStrong(this)] {
                            return TSharedRef::FromString(Data_);
                        })));
                    return;
                }

                auto readyEvent = RowReader_->GetReadyEvent();
                auto readyError = readyEvent.TryGet();
                if (!readyError) {
                    context->PropagateTo(readyEvent);
                    readyEvent.Subscribe(BIND(
                        &TEncodedRowStream::ContinueRead,
                        MakeStrong(this),
                        promise,
                        context)
                        .Via(GetSyncInvoker()));
                    return;
                }
                if (!readyError->IsOK()) {
                    promise.TrySet(*readyError);
                    return;
                }
            }
        } catch (const std::exception& ex) {
            promise.TrySet(TError(ex));
        }
    }

    /// NB(achains): Non-const members come first because async stream and format writer depends on them.
    TString Data_;
    TStringOutput OutputStream_;

    const IFlushableAsyncOutputStreamPtr AsyncOutputStream_;
    const ISchemalessFormatWriterPtr FormatWriter_;
    const IRowBatchReaderPtr RowReader_;
};

////////////////////////////////////////////////////////////////////////////////

IFormattedTableReaderPtr CreateEncodedRowStream(
    IRowBatchReaderPtr batchReader,
    NTableClient::TNameTablePtr nameTable,
    NFormats::TFormat format,
    NTableClient::TTableSchemaPtr tableSchema,
    std::optional<std::vector<std::string>> columns,
    NFormats::TControlAttributesConfigPtr controlAttributesConfig)
{
    return New<TEncodedRowStream>(
        std::move(batchReader),
        std::move(nameTable),
        std::move(format),
        std::move(tableSchema),
        std::move(columns),
        std::move(controlAttributesConfig));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
