#include "text_lenval.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/interface/transforms.h>

#include <yt/cpp/roren/interface/fns.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>

#include <util/stream/file.h>
#include <util/string/cast.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

void ParseTextLenval(IInputStream& input, TOutput<NBigRT::TMessageBatch>& output, const TReadTextLenvalOptions& options)
{
    i64 offset = 0;
    NBigRT::TMessageBatch messageBatch;

    auto flush = [&] {
        output.Add(messageBatch);
        messageBatch = NBigRT::TMessageBatch{};
    };


    TString textLength;

    while (input.ReadTo(textLength, '\n')) {
        size_t length;

        try {
            length = FromString<size_t>(textLength);
        } catch (const std::exception& ex) {
            ythrow yexception() << "bad text lenval format: cannot parse length prefix from: '" << textLength.substr(0, 10) << '\'';
        }

        TString data;
        data.ReserveAndResize(length);
        size_t loaded = input.Load(data.Detach(), data.length());
        if (loaded != length) {
            ythrow yexception() << "bad text lenval format: premature end of stream, expected size: " << length << ", loaded: " << loaded;
        }

        messageBatch.Messages.emplace_back();
        messageBatch.Messages.back().Data = std::move(data);
        messageBatch.Messages.back().Offset = offset++;

        if (static_cast<int>(messageBatch.Messages.size()) >= options.MessageBatchSize) {
            flush();
        }

        char ch;
        if (!input.ReadChar(ch)) {
            ythrow yexception() << "bad text lenval format: premature end of stream, expected '\\n' after chunk";
        }

        if (ch != '\n') {
            ythrow yexception() << "bad text lenval format: expected '\\n' after chunk, but found: '" << ch << "'";
        }
    }

    flush();
}

class TReadTextLenvalFromFileParDo
    : public IDoFn<int, NBigRT::TMessageBatch>
{
public:
    TReadTextLenvalFromFileParDo() = default;

    TReadTextLenvalFromFileParDo(TString fileName, TReadTextLenvalOptions options)
        : FileName_(std::move(fileName))
        , Options_(std::move(options))
    { }

    void Do(const int&, TOutput<NBigRT::TMessageBatch>& ) override
    { }

    void Finish(TOutput<NBigRT::TMessageBatch>& output) override
    {
        auto inputStream = TFileInput{FileName_};

        ParseTextLenval(inputStream, output, Options_);
    }

private:
    TString FileName_;
    TReadTextLenvalOptions Options_;

    Y_SAVELOAD_DEFINE_OVERRIDE(FileName_, Options_);
};

class TReadTextLenvalFromMemoryParDo
    : public IDoFn<int, NBigRT::TMessageBatch>
{
public:
    TReadTextLenvalFromMemoryParDo() = default;

    TReadTextLenvalFromMemoryParDo(TString data, TReadTextLenvalOptions options)
        : Data_(std::move(data))
        , Options_(std::move(options))
    { }

    void Do(const int&, TOutput<NBigRT::TMessageBatch>& ) override
    { }

    void Finish(TOutput<NBigRT::TMessageBatch>& output) override
    {
        auto inputStream = TMemoryInput{Data_};

        ParseTextLenval(inputStream, output, Options_);
    }

private:
    TString Data_;
    TReadTextLenvalOptions Options_;

    Y_SAVELOAD_DEFINE_OVERRIDE(Data_, Options_);
};

////////////////////////////////////////////////////////////////////////////////

class TNullRead
    : public NPrivate::IRawRead
{
    virtual std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {};
    }

    virtual std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TTypeTag<int>("null-read-output")};
    }

    const void* NextRaw() override
    {
        return nullptr;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> NPrivate::IRawReadPtr {
            return MakeIntrusive<TNullRead>();
        };
    }

    void SaveState(IOutputStream&) const override
    { }

    void LoadState(IInputStream&) override
    { }
};


TTransform<void, NBigRT::TMessageBatch> ReadTextLenvalFromFile(TString fileName, const TReadTextLenvalOptions& options)
{
    return [fileName=fileName, options=options] (const TPipeline& pipeline) -> TPCollection<NBigRT::TMessageBatch> {
        return pipeline
            | TReadTransform<int>(MakeIntrusive<TNullRead>())
            | MakeParDo<TReadTextLenvalFromFileParDo>(fileName, options);
    };
}

TTransform<void, NBigRT::TMessageBatch> ReadTextLenvalFromMemory(TString data, const TReadTextLenvalOptions& options)
{
    return [data=data, options=options] (const TPipeline& pipeline) -> TPCollection<NBigRT::TMessageBatch> {
        return pipeline
            | TReadTransform<int>(MakeIntrusive<TNullRead>())
            | MakeParDo<TReadTextLenvalFromMemoryParDo>(data, options);
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
