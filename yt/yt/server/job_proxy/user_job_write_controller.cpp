#include "user_job_write_controller.h"
#include "job.h"

#include <yt/yt/server/lib/misc/job_table_schema.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/user_job_io_factory.h>

#include <yt/yt/ytlib/table_client/blob_table_writer.h>
#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/finally.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NYTree;

using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

// Allows to write to single underlying writer rows
// that correspond to several streams and possibly have different schemas.
DECLARE_REFCOUNTED_CLASS(TMultiplexingWriter)

class TMultiplexingWriter
    : public TRefCounted
{
public:
    TMultiplexingWriter(
        TTypeConversionConfigPtr typeConversionConfig,
        ISchemalessMultiChunkWriterPtr underlying)
        : TypeConversionConfig_(std::move(typeConversionConfig))
        , Underlying_(std::move(underlying))
        , SerializedInvoker_(CreateSerializedInvoker(GetCurrentInvoker(), "multiplexing_writer"))
    { }

    // Create consumer corresponding to a stream no. |index| and having given schema.
    // Returned consumers can be safely used from different threads.
    std::unique_ptr<IFlushableValueConsumer> CreateConsumer(int index, TTableSchemaPtr schema)
    {
        YT_VERIFY(!Closed_);
        ++OpenWriterCount_;
        return std::make_unique<TStreamWritingValueConsumer>(
            this,
            index,
            Underlying_->GetNameTable(),
            std::move(schema),
            TypeConversionConfig_);
    }

private:
    const TTypeConversionConfigPtr TypeConversionConfig_;
    const ISchemalessMultiChunkWriterPtr Underlying_;
    const IInvokerPtr SerializedInvoker_;

    bool Closed_ = false;
    int OpenWriterCount_ = 0;

private:
    using TMultiplexingWriterPtr = TIntrusivePtr<TMultiplexingWriter>;

    struct TStreamWritingValueConsumerBufferTag
    { };

    class TStreamWritingValueConsumer
        : public IFlushableValueConsumer
        , public TValueConsumerBase
    {
    public:
        TStreamWritingValueConsumer(
            TMultiplexingWriterPtr owner,
            int tableIndex,
            TNameTablePtr nameTable,
            TTableSchemaPtr schema,
            TTypeConversionConfigPtr typeConversionConfig)
            : TValueConsumerBase(std::move(schema), std::move(typeConversionConfig))
            , Owner_(std::move(owner))
            , NameTable_(std::move(nameTable))
            , RowBuffer_(New<TRowBuffer>(TStreamWritingValueConsumerBufferTag()))
            , TableIndexValue_(MakeUnversionedInt64Value(
                tableIndex,
                NameTable_->GetIdOrRegisterName(TableIndexColumnName)))
        {
            InitializeIdToTypeMapping();
        }

        TFuture<void> Flush() override
        {
            if (RowBuffer_->GetSize() == 0) {
                return VoidFuture;
            }

            return
                BIND([writer = Owner_->Underlying_, rowBuffer = RowBuffer_, rows = std::move(Rows_)] {
                    writer->Write(rows);
                    rowBuffer->Clear();
                    return writer->GetReadyEvent();
                })
                .AsyncVia(Owner_->SerializedInvoker_)
                .Run();
        }

        const TNameTablePtr& GetNameTable() const override
        {
            return NameTable_;
        }

        bool GetAllowUnknownColumns() const override
        {
            return true;
        }

        void OnBeginRow() override
        { }

        void OnMyValue(const TUnversionedValue& value) override
        {
            RowBuilder_.AddValue(RowBuffer_->CaptureValue(value));
        }

        void OnEndRow() override
        {
            RowBuilder_.AddValue(TableIndexValue_);
            auto row = RowBuffer_->CaptureRow(RowBuilder_.GetRow(), /*capturevalues*/ false);
            Rows_.push_back(row);
            RowBuilder_.Reset();

            if (RowBuffer_->GetSize() >= MaxRowBufferSize_) {
                auto error = WaitFor(Flush());
                THROW_ERROR_EXCEPTION_IF_FAILED(error, "Table writer failed")
            }
        }

    private:
        static constexpr i64 MaxRowBufferSize_ = 1_MB;

        const TMultiplexingWriterPtr Owner_;
        const TNameTablePtr NameTable_;
        const TRowBufferPtr RowBuffer_;
        const TUnversionedValue TableIndexValue_;

        TUnversionedRowBuilder RowBuilder_;
        std::vector<TUnversionedRow> Rows_;
    };
};

DEFINE_REFCOUNTED_TYPE(TMultiplexingWriter)

////////////////////////////////////////////////////////////////////////////////

TUserJobWriteController::TUserJobWriteController(IJobHostPtr host)
    : Host_(host)
    , Logger(host->GetLogger())
{ }

TUserJobWriteController::~TUserJobWriteController() = default;

void TUserJobWriteController::Init()
{
    YT_LOG_INFO("Opening writers");

    auto guard = Finally([&] {
        Initialized_ = true;
    });

    auto userJobIOFactory = CreateUserJobIOFactory(
        Host_->GetJobSpecHelper(),
        TClientChunkReadOptions(),
        Host_->GetChunkReaderHost(),
        Host_->GetLocalHostName(),
        Host_->GetOutBandwidthThrottler());

    const auto& jobSpecExt = Host_->GetJobSpecHelper()->GetJobSpecExt();
    auto outputTransactionId = FromProto<TTransactionId>(jobSpecExt.output_transaction_id());

    TDataSinkDirectoryPtr dataSinkDirectory = nullptr;
    if (auto dataSinkDirectoryExt = FindProtoExtension<TDataSinkDirectoryExt>(jobSpecExt.extensions())) {
        dataSinkDirectory = FromProto<TDataSinkDirectoryPtr>(*dataSinkDirectoryExt);
        YT_VERIFY(std::ssize(dataSinkDirectory->DataSinks()) == jobSpecExt.output_table_specs_size());
    }

    for (int index = 0; index < jobSpecExt.output_table_specs_size(); ++index) {
        const auto& outputSpec = jobSpecExt.output_table_specs(index);
        auto dataSink = dataSinkDirectory ? std::make_optional(dataSinkDirectory->DataSinks()[index]) : std::nullopt;
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->EnableValidationOptions();

        auto ioConfig = Host_->GetJobSpecHelper()->GetJobIOConfig();
        auto writerConfig = outputSpec.dynamic()
            ? ioConfig->DynamicTableWriter
            : ioConfig->TableWriter;
        if (outputSpec.has_table_writer_config()) {
            writerConfig = UpdateYsonStruct(
                writerConfig,
                ConvertTo<INodePtr>(TYsonString(outputSpec.table_writer_config())));
        }

        auto timestamp = static_cast<TTimestamp>(outputSpec.timestamp());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        TTableSchemaPtr schema;
        DeserializeFromWireProto(&schema, outputSpec.table_schema());

        // ToDo(psushin): open writers in parallel.
        auto writer = userJobIOFactory->CreateWriter(
            Host_->GetClient(),
            writerConfig,
            options,
            chunkListId,
            outputTransactionId,
            schema,
            TChunkTimestamps{timestamp, timestamp},
            dataSink);

        Writers_.push_back(writer);
    }

    if (jobSpecExt.user_job_spec().has_stderr_table_spec()) {
        const auto& stderrTableSpec = jobSpecExt.user_job_spec().stderr_table_spec();
        const auto& outputTableSpec = stderrTableSpec.output_table_spec();
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(stderrTableSpec.output_table_spec().table_writer_options()));
        options->EnableValidationOptions();

        auto stderrTableWriterConfig = ConvertTo<TBlobTableWriterConfigPtr>(
            TYsonString(stderrTableSpec.blob_table_writer_config()));

        auto debugTransactionId = FromProto<TTransactionId>(jobSpecExt.user_job_spec().debug_transaction_id());

        StderrTableWriter_.reset(
            new NTableClient::TBlobTableWriter(
                GetStderrBlobTableSchema(),
                {ConvertToYsonString(Host_->GetJobId())},
                Host_->GetClient(),
                stderrTableWriterConfig,
                options,
                debugTransactionId,
                /*dataSink*/ std::nullopt,
                FromProto<TChunkListId>(outputTableSpec.chunk_list_id()),
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler()));
    }
}

std::vector<ISchemalessMultiChunkWriterPtr> TUserJobWriteController::GetWriters() const
{
    if (Initialized_) {
        return Writers_;
    } else {
        return {};
    }
}

int TUserJobWriteController::GetOutputStreamCount() const
{
    int count = 0;
    for (const auto& outputTableSpec : Host_->GetJobSpecHelper()->GetJobSpecExt().output_table_specs()) {
        if (outputTableSpec.stream_schemas_size() > 0) {
            count += outputTableSpec.stream_schemas_size();
        } else {
            count += 1;
        }
    }
    return count;
}

std::vector<IValueConsumer*> TUserJobWriteController::CreateValueConsumers(
    TTypeConversionConfigPtr typeConversionConfig)
{
    if (!Initialized_) {
        return {};
    }

    const auto& jobSpecExt = Host_->GetJobSpecHelper()->GetJobSpecExt();

    std::vector<IValueConsumer*> consumers;
    consumers.reserve(jobSpecExt.output_table_specs_size());
    YT_VERIFY(std::ssize(Writers_) == jobSpecExt.output_table_specs_size());
    for (int outputIndex = 0; outputIndex < jobSpecExt.output_table_specs_size(); ++outputIndex) {
        const auto& outputSpec = jobSpecExt.output_table_specs(outputIndex);
        const auto& writer = Writers_[outputIndex];
        if (outputSpec.stream_schemas_size() > 1) {
            // NB(levysotsky): Currently only first output is supposed to have several streams.
            YT_VERIFY(outputIndex == 0);

            auto multiplexingWriter = New<TMultiplexingWriter>(typeConversionConfig, writer);
            for (int streamIndex = 0; streamIndex < outputSpec.stream_schemas_size(); ++streamIndex) {
                TTableSchemaPtr schema;
                DeserializeFromWireProto(&schema, outputSpec.stream_schemas(streamIndex));
                ValueConsumers_.push_back(multiplexingWriter->CreateConsumer(streamIndex, schema));
                consumers.push_back(ValueConsumers_.back().get());
            }
        } else {
            ValueConsumers_.push_back(std::make_unique<TWritingValueConsumer>(writer, typeConversionConfig));
            consumers.push_back(ValueConsumers_.back().get());
        }
    }
    return consumers;
}

const std::vector<std::unique_ptr<IFlushableValueConsumer>>& TUserJobWriteController::GetAllValueConsumers() const
{
    return ValueConsumers_;
}

IOutputStream* TUserJobWriteController::GetStderrTableWriter() const
{
    if (Initialized_) {
        return StderrTableWriter_.get();
    } else {
        return nullptr;
    }
}

void TUserJobWriteController::PopulateResult(TJobResultExt* jobResultExt)
{
    std::vector<NChunkClient::NProto::TChunkSpec> writtenChunkSpecs;
    const auto& outputTableSpecs = Host_->GetJobSpecHelper()->GetJobSpecExt().output_table_specs();
    for (int index = 0; index < std::ssize(Writers_); ++index) {
        const auto& writer = Writers_[index];
        const auto& outputTableSpec = outputTableSpecs.Get(index);
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputTableSpec.table_writer_options()));
        if (options->ReturnBoundaryKeys) {
            *jobResultExt->add_output_boundary_keys() = GetWrittenChunksBoundaryKeys(writer);
        }
        for (const auto& chunkSpec : writer->GetWrittenChunkSpecs()) {
            writtenChunkSpecs.push_back(chunkSpec);
            FilterProtoExtensions(writtenChunkSpecs.back().mutable_chunk_meta()->mutable_extensions(), GetSchedulerChunkMetaExtensionTagsFilter());
        }
    }
    ToProto(jobResultExt->mutable_output_chunk_specs(), writtenChunkSpecs);
}

void TUserJobWriteController::PopulateStderrResult(NControllerAgent::NProto::TJobResultExt* jobResultExt)
{
    if (StderrTableWriter_) {
        *jobResultExt->mutable_stderr_table_boundary_keys() = StderrTableWriter_->GetOutputResult();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
