#include "journal_commands.h"
#include "config.h"

#include <yt/ytlib/api/journal_reader.h>
#include <yt/ytlib/api/journal_writer.h>

#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/formats/format.h>
#include <yt/ytlib/formats/parser.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/blob_output.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYson;
using namespace NYTree;
using namespace NFormats;
using namespace NConcurrency;
using namespace NChunkClient;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TReadJournalCommand::TReadJournalCommand()
{
    RegisterParameter("path", Path);
    RegisterParameter("journal_reader", JournalReader)
        .Default();

    RegisterPostprocessor([&] {
        Path = Path.Normalize();
    });
}

void TReadJournalCommand::DoExecute(ICommandContextPtr context)
{
    auto checkLimit = [] (const TReadLimit& limit) {
        if (limit.HasKey()) {
            THROW_ERROR_EXCEPTION("Reading key range is not supported in journals");
        }
        if (limit.HasChunkIndex()) {
            THROW_ERROR_EXCEPTION("Reading chunk index range is not supported in journals");
        }
        if (limit.HasOffset()) {
            THROW_ERROR_EXCEPTION("Reading offset range is not supported in journals");
        }
    };

    if (Path.GetRanges().size() > 1) {
        THROW_ERROR_EXCEPTION("Reading multiple ranges is not supported in journals");
    }

    Options.Config = UpdateYsonSerializable(
        context->GetConfig()->JournalReader,
        JournalReader);

    if (Path.GetRanges().size() == 1) {
        auto range = Path.GetRanges()[0];

        checkLimit(range.LowerLimit());
        checkLimit(range.UpperLimit());

        Options.FirstRowIndex = range.LowerLimit().HasRowIndex()
            ? range.LowerLimit().GetRowIndex()
            : 0;

        if (range.UpperLimit().HasRowIndex()) {
            Options.RowCount = range.UpperLimit().GetRowIndex() - *Options.FirstRowIndex;
        }
    }

    auto reader = context->GetClient()->CreateJournalReader(
        Path.GetPath(),
        Options);

    WaitFor(reader->Open())
        .ThrowOnError();

    auto output = context->Request().OutputStream;

    // TODO(babenko): provide custom allocation tag
    TBlobOutput buffer;
    auto flushBuffer = [&] () {
        WaitFor(output->Write(buffer.Flush()))
            .ThrowOnError();
    };

    auto format = context->GetOutputFormat();
    auto consumer = CreateConsumerForFormat(format, EDataType::Tabular, &buffer);

    while (true) {
        auto rowsOrError = WaitFor(reader->Read());
        const auto& rows = rowsOrError.ValueOrThrow();

        if (rows.empty())
            break;

        for (auto row : rows) {
            BuildYsonListFluently(consumer.get())
                .Item().BeginMap()
                    .Item("data").Value(TStringBuf(row.Begin(), row.Size()))
                .EndMap();
        }

        if (buffer.Size() > context->GetConfig()->ReadBufferSize) {
            flushBuffer();
        }
    }

    consumer->Flush();

    if (buffer.Size() > 0) {
        flushBuffer();
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJournalConsumerState,
    (Root)
    (AtItem)
    (InsideMap)
    (AtData)
);

class TJournalConsumer
    : public TYsonConsumerBase
{
public:
    explicit TJournalConsumer(IJournalWriterPtr writer)
        : Writer_(writer)
    { }

private:
    IJournalWriterPtr Writer_;

    EJournalConsumerState State_ = EJournalConsumerState::Root;


    virtual void OnStringScalar(TStringBuf value) override
    {
        if (State_ != EJournalConsumerState::AtData) {
            ThrowMalformedData();
        }

        std::vector<TSharedRef> rows;
        rows.push_back(TSharedRef::FromString(TString(value)));

        WaitFor(Writer_->Write(rows))
            .ThrowOnError();

        State_ = EJournalConsumerState::InsideMap;
    }

    virtual void OnInt64Scalar(i64 /*value*/) override
    {
        ThrowMalformedData();
    }

    virtual void OnUint64Scalar(ui64 /*value*/) override
    {
        ThrowMalformedData();
    }

    virtual void OnDoubleScalar(double /*value*/) override
    {
        ThrowMalformedData();
    }

    virtual void OnBooleanScalar(bool /*value*/) override
    {
        ThrowMalformedData();
    }

    virtual void OnEntity() override
    {
        ThrowMalformedData();
    }

    virtual void OnBeginList() override
    {
        ThrowMalformedData();
    }

    virtual void OnListItem() override
    {
        if (State_ != EJournalConsumerState::Root) {
            ThrowMalformedData();
        }
        State_ = EJournalConsumerState::AtItem;
    }

    virtual void OnEndList() override
    {
        Y_UNREACHABLE();
    }

    virtual void OnBeginMap() override
    {
        if (State_ != EJournalConsumerState::AtItem) {
            ThrowMalformedData();
        }
        State_ = EJournalConsumerState::InsideMap;
    }

    virtual void OnKeyedItem(TStringBuf key) override
    {
        if (State_ != EJournalConsumerState::InsideMap) {
            ThrowMalformedData();
        }
        if (key != AsStringBuf("data")) {
            ThrowMalformedData();
        }
        State_ = EJournalConsumerState::AtData;
    }

    virtual void OnEndMap() override
    {
        if (State_ != EJournalConsumerState::InsideMap) {
            ThrowMalformedData();
        }
        State_ = EJournalConsumerState::Root;
    }

    virtual void OnBeginAttributes() override
    {
        ThrowMalformedData();
    }

    virtual void OnEndAttributes() override
    {
        Y_UNREACHABLE();
    }


    void ThrowMalformedData()
    {
        THROW_ERROR_EXCEPTION("Malformed journal data");
    }

};

TWriteJournalCommand::TWriteJournalCommand()
{
    RegisterParameter("path", Path);
}

void TWriteJournalCommand::DoExecute(ICommandContextPtr context)
{
    Options.Config = UpdateYsonSerializable(
        context->GetConfig()->JournalWriter,
        JournalWriter);

    auto writer = context->GetClient()->CreateJournalWriter(
        Path.GetPath(),
        Options);

    WaitFor(writer->Open())
        .ThrowOnError();

    TJournalConsumer consumer(writer);

    auto format = context->GetInputFormat();
    auto parser = CreateParserForFormat(format, EDataType::Tabular, &consumer);

    struct TWriteBufferTag { };

    auto buffer = TSharedMutableRef::Allocate<TWriteBufferTag>(context->GetConfig()->WriteBufferSize, false);

    auto input = context->Request().InputStream;

    while (true) {
        auto bytesRead = WaitFor(input->Read(buffer))
            .ValueOrThrow();

        if (bytesRead == 0)
            break;

        parser->Read(TStringBuf(buffer.Begin(), bytesRead));
    }

    parser->Finish();

    WaitFor(writer->Close())
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
