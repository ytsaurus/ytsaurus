#include "encoded_row_stream.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/api/formatted_table_reader.h>
#include <yt/yt/client/api/row_batch_reader.h>
#include <yt/yt/client/api/table_reader.h>

#include <yt/yt/core/concurrency/async_stream_helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/string/stream.h>

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
        TControlAttributesConfigPtr controlAttributesConfig,
        NProfiling::TWallTimer totalTimer)
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
        , TableReader_(DynamicPointerCast<ITableReader>(RowReader_))
        , TotalTimer_(std::move(totalTimer))
    { }

    TFuture<TSharedRef> Read() override
    {
        auto batch = RowReader_->Read();

        if (batch && !batch->IsEmpty()) {
            Data_.clear();

            FormatWriter_->WriteBatch(batch);

            return FormatWriter_->Flush().Apply(
                BIND([=, this, this_ = MakeStrong(this)] {
                    return TSharedRef::FromString(Data_);
                }));
        }

        if (batch) {
            return RowReader_->GetReadyEvent().Apply(
                BIND(&TEncodedRowStream::Read, MakeStrong(this)));
        }

        return MakeFuture(TSharedRef());
    }

    TTableReaderTimingStatistics GetTimingStatistics() const override
    {
        TTableReaderTimingStatistics statistics{
            .TotalTime = TotalTimer_.GetElapsedTime(),
        };
        // Table partition readers do not track timing, so only the total time is reported for them.
        if (TableReader_) {
            auto readerStatistics = TableReader_->GetTimingStatistics();
            statistics.MasterFetchTime = readerStatistics.MasterFetchTime;
            statistics.DataReadTiming = readerStatistics.DataReadTiming;
        }
        return statistics;
    }

private:
    /// NB(achains): Non-const members come first because async stream and format writer depends on them.
    std::string Data_;
    TStdStringOutput OutputStream_;

    const IFlushableAsyncOutputStreamPtr AsyncOutputStream_;
    const ISchemalessFormatWriterPtr FormatWriter_;
    const IRowBatchReaderPtr RowReader_;
    const ITableReaderPtr TableReader_;
    const NProfiling::TWallTimer TotalTimer_;
};

////////////////////////////////////////////////////////////////////////////////

IFormattedTableReaderPtr CreateEncodedRowStream(
    IRowBatchReaderPtr batchReader,
    NTableClient::TNameTablePtr nameTable,
    NFormats::TFormat format,
    NTableClient::TTableSchemaPtr tableSchema,
    std::optional<std::vector<std::string>> columns,
    NFormats::TControlAttributesConfigPtr controlAttributesConfig,
    NProfiling::TWallTimer totalTimer)
{
    return New<TEncodedRowStream>(
        std::move(batchReader),
        std::move(nameTable),
        std::move(format),
        std::move(tableSchema),
        std::move(columns),
        std::move(controlAttributesConfig),
        std::move(totalTimer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
