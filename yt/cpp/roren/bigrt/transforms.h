#pragma once

#include "fwd.h"
#include "sensors.h"
#include "tagset_saveload.h"

#include <bigrt/lib/queue/message_batch/message_batch.h>

#include <library/cpp/framing/unpacker.h>

#include <logfeller/lib/chunk_splitter/record_context.h>

#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/fwd.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

struct TLogfellerRecord
{
    TString Record;
    NLogFeller::NChunkSplitter::TRecordContext RecordContext;
    i64 DataMessageOffset = -1;
    ui64 ChunkIndex;
    TInstant CreateTime;
    TInstant WriteTime;
};

inline void RorenEncode(IOutputStream*, const TLogfellerRecord&)
{
    Y_FAIL("not implemented");
}

inline void RorenDecode(IInputStream*, TLogfellerRecord&)
{
    Y_FAIL("not implemented");
}

struct TParseLogFellerOptions
{
    /// Name of sensor that counts input messages in TMessageBatch.
    TMaybe<TTaggedSensor> InputCountSensor;

    /// Name of sensor that counts bytes in input messages before unpacking.
    TMaybe<TTaggedSensor> InputCompressedBytesSensor;

    /// Name of sensor that counts successfully unpacked messages.
    TMaybe<TTaggedSensor> UnpackOkCountSensor;

    /// Name of sensor that counts messages that cannot be unpacked.
    TMaybe<TTaggedSensor> UnpackErrorCountSensor;

    /// Name of sensor that counts (unpacked) bytes in successfully unpacked messages.
    TMaybe<TTaggedSensor> UnpackedBytesSensor;

    /// Name of sensors that tracks total time of unpacking messages.
    TMaybe<TTaggedSensor> UnpackingTime;

    NYT::NProfiling::TTagSet ProfilerTags;

    Y_SAVELOAD_DEFINE(
        InputCountSensor,
        InputCompressedBytesSensor,
        UnpackOkCountSensor,
        UnpackErrorCountSensor,
        UnpackedBytesSensor,
        UnpackingTime,
        ProfilerTags
    );
};

///
/// @brief Creates ParDo for splitting by logfeller records
///
/// ParDo splits each message from message batch into several logfeller records
TParDoTransform<NBigRT::TMessageBatch, TLogfellerRecord> ParseLogfellerParDo(TString chunkSplitterFormat, TParseLogFellerOptions options = {});

////////////////////////////////////////////////////////////////////////////////

struct TUnpackMessagesOptions {
    /// Name of sensor that counts input messages in TMessageBatch.
    TMaybe<TTaggedSensor> InputCountSensor;

    /// Name of sensor that counts messages that cannot be unpacked.
    TMaybe<TTaggedSensor> UnpackErrorCountSensor;

    /// Name of sensor that counts successfuly unpacked messages.
    TMaybe<TTaggedSensor> UnpackOkCountSensor;

    Y_SAVELOAD_DEFINE(
        InputCountSensor,
        UnpackErrorCountSensor,
        UnpackOkCountSensor
    );
};

///
/// @brief Creates ParDo for unpacking messages from batch
///
/// ParDo unpacks each message from message batch
TParDoTransform<NBigRT::TMessageBatch, TString> UnpackMessagesParDo(TUnpackMessagesOptions options = {});

////////////////////////////////////////////////////////////////////////////////

struct TParseProtoOptions {
    /// Name of sensor that counts input messages.
    TMaybe<TTaggedSensor> InputCountSensor;

    /// Name of sensor that counts messages that cannot be parsed to proto message.
    TMaybe<TTaggedSensor> ParseErrorCountSensor;

    Y_SAVELOAD_DEFINE(
        InputCountSensor,
        ParseErrorCountSensor
    );
};

template <class TProtoMessage>
requires std::derived_from<TProtoMessage, ::google::protobuf::Message>
class TParseProtoFn : public IDoFn<TString, TProtoMessage>
{
public:
    TParseProtoFn() = default;

    TParseProtoFn(TParseProtoOptions options)
        : Options_(std::move(options))
    {
    }

    void Start(TOutput<TProtoMessage>&) override
    {
        Metrics_ = {};
    }

    void Do(const TString& data, TOutput<TProtoMessage>& output) override
    {
        TProtoMessage message;
        if (!message.ParseFromString(data)) {
            ++Metrics_.ParseErrorCount;
        } else {
            output.Add(message);
        }
    }

    void Finish(TOutput<TProtoMessage>&) override
    {
        TCounterUpdater updater(this->GetExecutionContext()->GetProfiler());
        updater.Update(Options_.InputCountSensor, Metrics_.InputCount);
        updater.Update(Options_.ParseErrorCountSensor, Metrics_.ParseErrorCount);
    }

private:
    struct TMetrics {
        i64 InputCount = 0;
        i64 ParseErrorCount = 0;
    };

private:
    TParseProtoOptions Options_;

    Y_SAVELOAD_DEFINE_OVERRIDE(Options_);

    TMetrics Metrics_;
};

///
/// @brief Creates ParDo for parsing string into proto
///
/// ParDo takes unpacked message (or any string) and parses it into proto message
template <class TProtoMessage>
TParDoTransform<TString, TProtoMessage> ParseProtoParDo(TParseProtoOptions options = {})
{
    return MakeParDo<TParseProtoFn<TProtoMessage>>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoMessage>
requires std::derived_from<TProtoMessage, ::google::protobuf::Message>
class TUnpackFramedMessagesDoFn
    : public NRoren::IDoFn<TString, TProtoMessage>
{
public:
    TUnpackFramedMessagesDoFn() = default;

    explicit TUnpackFramedMessagesDoFn(NFraming::EFormat format)
        : Format_(format)
    { }

    void Do(const TString& input, NRoren::TOutput<TProtoMessage>& output) override
    {
        auto unpacker = NFraming::TUnpacker(Format_, input);

        TProtoMessage message;
        TStringBuf skipped;

        while (unpacker.NextFrame(message, skipped)) {
            output.Add(message);
            message.Clear();
        }
    }

private:
    NFraming::EFormat Format_ = NFraming::EFormat::Auto;

    Y_SAVELOAD_DEFINE_OVERRIDE(Format_);
};

template <typename TProtoMessage>
NRoren::TTransform<TString, TProtoMessage> UnpackFramedMessagesParDo(NFraming::EFormat format = NFraming::EFormat::Auto)
{
    return MakeParDo<TUnpackFramedMessagesDoFn<TProtoMessage>>(format);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
