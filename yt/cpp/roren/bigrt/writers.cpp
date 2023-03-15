#include "writers.h"

#include "bigrt.h"
#include "bigrt_execution_context.h"
#include "composite_bigrt_writer.h"

#include <yt/cpp/roren/bigrt/graph/parser.h>
#include <yt/cpp/roren/interface/roren.h>

#include <bigrt/lib/writer/yt_queue/factory.h>
#include <bigrt/lib/writer/yt_dyntable/factory.h>

#include <quality/user_sessions/rt/lib/writers/logbroker/factory.h>
#include <quality/user_sessions/rt/lib/writers/logbroker/writer.h>

#include <yt/yt/library/tvm/service/public.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/wire_protocol.h>


#include <util/generic/guid.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TWriteStringToYtQueueParDo
    : public IDoFn<TKV<ui64, TString>, void>
{
public:
    using TTag = TTypeTag<NBigRT::IYtQueueWriter>;
public:
    TWriteStringToYtQueueParDo() = default;

    explicit TWriteStringToYtQueueParDo(TTag id)
        : WriterId_(std::move(id))
    { }

    void Start(TOutput<void>&) override
    {
        const auto& compositeWriter = NPrivate::GetWriter(GetExecutionContext()->As<IBigRtExecutionContext>());
        Writer_ = &compositeWriter->GetWriter(WriterId_);
    }

    void Do(const TKV<ui64, TString>& input, TOutput<void>&) override
    {
        const auto& [shard, string] = input;
        Writer_->Write(shard, string);
    }

    void Finish(TOutput<void>&) override
    {
        Writer_ = nullptr;
    }

    const TTypeTag<NBigRT::IYtQueueWriter>& GetTag() const
    {
        return WriterId_;
    }

private:
    TTag WriterId_ = TTag{"uninitialized"};

    NBigRT::IYtQueueWriter* Writer_ = nullptr;

    Y_SAVELOAD_DEFINE_OVERRIDE(WriterId_);
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TWriteToYtDynTable
    : public IDoFn<T, void>
{
public:
    using TBase = IDoFn<T, void>;
    using TBase::GetExecutionContext;
    using TWriter = NBigRT::TYtDynTableWriter;
    using TTag = TTypeTag<TWriter>;
public:
    TWriteToYtDynTable() = default;

    explicit TWriteToYtDynTable(TTag id)
        : WriterId_(std::move(id))
    {}

    void Start(TOutput<void>&) override
    {
        const auto& compositeWriter = NPrivate::GetWriter(GetExecutionContext()->template As<IBigRtExecutionContext>());
        Writer_ = &compositeWriter->GetWriter(WriterId_);
    }

    void Do(const T& input, TOutput<void>&) override
    {
        Writer_->Write(input);
    }

    void Finish(TOutput<void>&) override
    {
        Writer_ = nullptr;
    }

    const TTag& GetTag() const
    {
        return WriterId_;
    }

private:
    TTag WriterId_ = TTag{"uninitialized"};
    TWriter* Writer_ = nullptr;
    Y_SAVELOAD_DEFINE_OVERRIDE(WriterId_);
};

////////////////////////////////////////////////////////////////////////////////

class TWriteStringToLogbrokerParDo
    : public IDoFn<TLogbrokerData, void>
{
public:
    using TTag = TTypeTag<NUserSessions::NRT::ILogbrokerWriter>;

public:
    TWriteStringToLogbrokerParDo() = default;

    explicit TWriteStringToLogbrokerParDo(TTag tag)
        : WriterId_(std::move(tag))
    { }

    void Start(TOutput<void>&) override
    {
        const auto& compositeWriter = NPrivate::GetWriter(GetExecutionContext()->As<IBigRtExecutionContext>());
        Writer_ = &compositeWriter->GetWriter(WriterId_);
    }

    void Do(const TLogbrokerData& input, TOutput<void>&) override
    {
        Writer_->Write(input.Shard, input.Value, input.SeqNo, input.GroupIndex);
    }

    void Finish(TOutput<void>&) override
    {
        Writer_ = nullptr;
    }

private:
    TTag WriterId_ = TTag{"uninitialized"};

    NUserSessions::NRT::ILogbrokerWriter* Writer_ = nullptr;

    Y_SAVELOAD_DEFINE_OVERRIDE(WriterId_);
};

////////////////////////////////////////////////////////////////////////////////

void TWriteYtDynTableTransform::ApplyTo(const TPCollection<NYT::TNode>& pCollection) const
{
    ApplyTo<NYT::TNode>(pCollection);
}

void TWriteYtDynTableTransform::ApplyTo(const TPCollection<NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>>& pCollection) const
{
    ApplyTo<NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>>(pCollection);
}

template <class T>
void TWriteYtDynTableTransform::ApplyTo(const TPCollection<T>& pCollection) const
{
    using TWriter = TWriteToYtDynTable<T>;
    auto tag = typename TWriter::TTag{TGUID::Create().AsGuidString()}; // TODO: we actually need some info about our writer here to ease debug

    auto write = MakeParDo<TWriter>(tag);

    TWriterRegistrator writerRegistrator = [tag=std::move(tag), ytPath=YtPath_, nameTable=NameTable_] (
        TCompositeBigRtWriterFactory& factory,
        const NYT::NAuth::ITvmServicePtr&)
    {
        factory.Add(tag, NBigRT::CreateYtDynTableWriterFactory(ytPath, nameTable));
    };

    SetAttribute(write, WriterRegistratorTag, writerRegistrator);

    pCollection | write;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

namespace NRoren {

TTransform<TLogbrokerData, void> WriteRawLogbroker(NUserSessions::NRT::TLogbrokerWriterConfig config)
{
    auto configPtr =std::make_shared<const NUserSessions::NRT::TLogbrokerWriterConfig>(std::move(config));
    return [configPtr=configPtr] (const TPCollection<TLogbrokerData>& pCollection) -> void {
        using namespace NPrivate;

        auto tag = TWriteStringToLogbrokerParDo::TTag{TGUID::Create().AsGuidString()}; // TODO: we actually need some info about our writer here to ease debug
        auto write = MakeParDo<TWriteStringToLogbrokerParDo>(tag);

        TWriterRegistrator writerRegistrator = [tag = tag, config = configPtr] (
            TCompositeBigRtWriterFactory& factory,
            const NYT::NAuth::ITvmServicePtr& tvmService)
        {
            factory.Add(tag, NUserSessions::NRT::CreateLogbrokerWriterFactory(*config, tvmService));
        };

        SetAttribute(write, WriterRegistratorTag, writerRegistrator);

        pCollection | write;
    };
}

////////////////////////////////////////////////////////////////////////////////

TTransform<TKV<ui64, TString>, void> WriteRawYtQueue(NBigRT::TYtQueueWriterConfig config)
{
    return [
        configPtr=std::make_shared<const NBigRT::TYtQueueWriterConfig>(std::move(config))
    ] (const TPCollection<TKV<ui64, TString>>& pCollection) {
        using namespace NPrivate;

        auto tag = TTypeTag<NBigRT::IYtQueueWriter>{TGUID::Create().AsGuidString()}; // TODO: we actually need some info about our writer here to ease debug

        auto write = MakeParDo<TWriteStringToYtQueueParDo>(tag);

        TWriterRegistrator writerRegistrator = [tag = tag, config = configPtr] (
            TCompositeBigRtWriterFactory& factory,
            const NYT::NAuth::ITvmServicePtr&)
        {
            factory.Add(tag, NBigRT::CreateYtQueueWriterFactory(*config));
        };

        SetAttribute(write, WriterRegistratorTag, writerRegistrator);

        pCollection | write;
    };
}

////////////////////////////////////////////////////////////////////////////////

void RorenEncode(IOutputStream* out, const NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>& rows)
{
    std::unique_ptr<NYT::NTableClient::IWireProtocolWriter> writer = NYT::NTableClient::CreateWireProtocolWriter();
    writer->WriteUnversionedRowset(rows);
    std::vector<NYT::TSharedRef> datas = writer->Finish();
    TString result;
    for (const NYT::TSharedRef& data : datas) {
        result += TStringBuf(data.begin(), data.end());
    }
    ::Save(out, result);
}

void RorenDecode(IInputStream* in, NYT::TSharedRange<NYT::NTableClient::TUnversionedRow>& result)
{
    TString strbuf;
    ::Load(in, strbuf);
    NYT::TSharedRef data(strbuf.begin(), strbuf.end(), {});
    NYT::NTableClient::TRowBufferPtr buffer = NYT::New<NYT::NTableClient::TRowBuffer>();
    std::unique_ptr<NYT::NTableClient::IWireProtocolReader> reader = NYT::NTableClient::CreateWireProtocolReader(data, buffer);
    result = reader->ReadUnversionedRowset(true /*captureValues*/);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
