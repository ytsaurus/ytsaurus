#pragma once

#include "bigrt_execution_context.h"
#include "destination_writer.h"
#include "fwd.h"

#include <yt/cpp/roren/interface/fns.h>
#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/interface/transforms.h>
#include <yt/cpp/roren/library/bind_par_do/bind.h>
#include <yt/cpp/roren/library/tagset_saveload/tagset_saveload.h>

#include <bigrt/lib/processing/resharder/shard_number/shard_number.h>
#include <bigrt/lib/queue/message_batch/message_batch.h>
#include <yt/cpp/roren/library/logger/logger.h>

#include <library/cpp/framing/packer.h>
#include <library/cpp/framing/unpacker.h>

#include <util/generic/size_literals.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <typename TMetricsStruct>
struct TMetricsOptions
{
    using TMetrics = TMetricsStruct;
    using TMetricsFactory = TMetrics(*)(const NYT::NProfiling::TProfiler&);

    NYT::NProfiling::TTagSet ProfilerTags = {};
    TMetricsFactory MetricsFactory = [] (const NYT::NProfiling::TProfiler& profiler) -> TMetrics {
        return TMetrics{profiler};
    };

    Y_SAVELOAD_DEFINE(ProfilerTags, NPrivate::SaveLoadablePointer(MetricsFactory));
};

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

struct TUnpackMessagesMetrics {
    NYT::NProfiling::TProfiler Profiler;
    NYT::NProfiling::TCounter InputCount = Profiler.Counter(".records.input.total");
    NYT::NProfiling::TCounter UnpackOkCount = Profiler.Counter(".records.input.valid");
    NYT::NProfiling::TCounter UnpackErrorCount = Profiler.Counter(".records.input.broken");
};
using TUnpackMessagesOptions = NPrivate::TMetricsOptions<TUnpackMessagesMetrics>;

///
/// @brief Creates ParDo for unpacking messages from batch
///
/// ParDo unpacks each message from message batch
TParDoTransform<NBigRT::TMessageBatch, TString> UnpackMessagesParDo(TUnpackMessagesOptions options = {});

////////////////////////////////////////////////////////////////////////////////

struct TParseProtoMetrics {
    NYT::NProfiling::TProfiler Profiler;
    NYT::NProfiling::TCounter ValidInputCount = Profiler.Counter(".parse_proto.valid_count");
    NYT::NProfiling::TCounter ParseErrorCount = Profiler.Counter(".parse_proto.error_count");
};
using TParseProtoOptions = NPrivate::TMetricsOptions<TParseProtoMetrics>;

template <class TProtoMessage>
requires std::derived_from<TProtoMessage, ::google::protobuf::Message>
class TParseProtoFn : public IDoFn<TString, TProtoMessage>
{
public:
    using TMetrics = TParseProtoOptions::TMetrics;

    TParseProtoFn() = default;

    TParseProtoFn(TParseProtoOptions options)
        : Options_(std::move(options))
    {
    }

    void Start(TOutput<TProtoMessage>&) override
    {
        MetricIncrements_ = {};
        if (!Metrics_) {
            Metrics_ = Options_.MetricsFactory(this->GetExecutionContext()->GetProfiler().WithTags(Options_.ProfilerTags));
        }
    }

    void Do(const TString& data, TOutput<TProtoMessage>& output) override
    {
        TProtoMessage message;
        if (!message.ParseFromString(data)) {
            ++MetricIncrements_.ParseErrorCount;
        } else {
            ++MetricIncrements_.ValidInputCount;
            output.Add(message);
        }
    }

    void Finish(TOutput<TProtoMessage>&) override
    {
        MetricIncrements_.Flush(*Metrics_);
    }

    NRoren::TFnAttributes GetDefaultAttributes() const override
    {
        NRoren::TFnAttributes attrs;
        attrs.SetIsPure();
        return attrs;
    }

private:
    struct TMetricIncrements
    {
        uint64_t ValidInputCount = 0;
        uint64_t ParseErrorCount = 0;

        void Flush(const TMetrics& metrics) const
        {
            metrics.ValidInputCount.Increment(ValidInputCount);
            metrics.ParseErrorCount.Increment(ParseErrorCount);
        }
    };

private:
    TParseProtoOptions Options_;

    Y_SAVELOAD_DEFINE_OVERRIDE(Options_);

    TMetricIncrements MetricIncrements_;
    TMaybe<TMetrics> Metrics_;
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

template <typename TMessage>
class TUnpackFramedMessagesDoFn
    : public NRoren::IDoFn<TString, TMessage>
{
public:
    TUnpackFramedMessagesDoFn() = default;

    explicit TUnpackFramedMessagesDoFn(NFraming::EFormat format)
        : Format_(format)
    { }

    void Do(const TString& input, NRoren::TOutput<TMessage>& output) override
    {
        USE_ROREN_LOGGER();
        auto unpacker = NFraming::TUnpacker(Format_, input);

        TMessage message;
        TStringBuf skipped;

        while (unpacker.NextFrame(message, skipped)) {
            YT_LOG_ERROR_IF(!skipped.Empty(), "Error during decompression of framed messages, skipped data: `%v`", skipped);
            output.Add(message);
            message.Clear();
        }
        YT_LOG_ERROR_IF(!skipped.Empty(), "Error during decompression of framed messages, skipped data: `%v`", skipped);
    }

    NRoren::TFnAttributes GetDefaultAttributes() const override
    {
        NRoren::TFnAttributes attrs;
        attrs.SetIsPure();
        return attrs;
    }

private:
    NFraming::EFormat Format_ = NFraming::EFormat::Auto;

    Y_SAVELOAD_DEFINE_OVERRIDE(Format_);
};

template <typename TMessage>
NRoren::TTransform<TString, TMessage> UnpackFramedMessagesParDo(NFraming::EFormat format = NFraming::EFormat::Auto)
{
    return MakeParDo<TUnpackFramedMessagesDoFn<TMessage>>(format);
}

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TStringReslicer
{
public:
    using TConsumer = std::function<void(const TString&)>;
public:
    TStringReslicer() = default;
    TStringReslicer(const TStringReslicer&) = delete;

    void Reset();
    void Add(const TString& value, size_t maxSize, const TConsumer& output);
    void Flush(const TConsumer& output);

private:
    TString Buffer_;
};

class TFrameReslicer
{
public:
    using TConsumer = std::function<void(TString&&)>;

    TFrameReslicer(NFraming::EFormat format, size_t maxFrameSize, size_t maxMessageCount);

    void Add(const google::protobuf::Message& message, TConsumer consumer);

    void Flush(TConsumer consumer);

private:
    TStringStream Stream_;
    NFraming::TPacker Packer_;
    const size_t MaxFrameSize_;
    const size_t MaxMessageCount_;

    size_t MessageCount_ = 0;

    void DoFlush(TConsumer& consumer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoMessage>
    requires std::derived_from<TProtoMessage, ::google::protobuf::Message>
class TPackFramedMessagesDoFn
    : public IDoFn<TProtoMessage, TString>
{
public:
    TPackFramedMessagesDoFn() = default;

    TPackFramedMessagesDoFn(NFraming::EFormat format, size_t maxSize)
        : Format_(format)
        , MaxSize_(maxSize)
    { }

    void Start(TOutput<TString>&) override
    {
        Reslicer_.Reset();
    }

    void Do(const TProtoMessage& input, TOutput<TString>& output) override
    {
        auto packed = NFraming::PackToString(Format_, input);
        Reslicer_.Add(packed, MaxSize_, [&] (const TString& value) {
            output.Add(value);
        });
    }

    void Finish(TOutput<TString>& output) override
    {
        Reslicer_.Flush([&] (const TString& value) {
            output.Add(value);
        });
    }

private:
    NFraming::EFormat Format_ = NFraming::EFormat::Auto;
    size_t MaxSize_ = 8_MB;

    Y_SAVELOAD_DEFINE_OVERRIDE(Format_, MaxSize_);

    NPrivate::TStringReslicer Reslicer_;
};

class TPackFramedMessagesTransform
{
public:
    TPackFramedMessagesTransform(NFraming::EFormat format, size_t maxSize);

    TString GetName() const
    {
        return "PackFramedMessages";
    }

    template <typename T>
        requires std::is_base_of_v<::google::protobuf::Message, T>
    auto ApplyTo(const TPCollection<T>& pCollection) const
    {
        return pCollection | MakeParDo<TPackFramedMessagesDoFn<T>>(Format_, MaxSize_);
    }

private:
    NFraming::EFormat Format_;
    size_t MaxSize_;
};

TPackFramedMessagesTransform PackFramedMessagesParDo(NFraming::EFormat format = NFraming::EFormat::Protoseq, size_t maxSize = 8_MB);

////////////////////////////////////////////////////////////////////////////////

template <typename K, typename TProtoMessage>
    requires std::derived_from<TProtoMessage, ::google::protobuf::Message>
class TPackFramedMessagesPerKeyDoFn
    : public IDoFn<TKV<K, TProtoMessage>, TKV<K, TString>>
{
public:
    TPackFramedMessagesPerKeyDoFn() = default;

    TPackFramedMessagesPerKeyDoFn(NFraming::EFormat format, size_t maxFrameSize, size_t maxMessageCount)
        : Format_(format)
        , MaxFrameSize_(maxFrameSize)
        , MaxMessageCount_(maxMessageCount)
    { }

    void Start(TOutput<TKV<K, TString>>&) override
    {
        ReslicerMap_.clear();
    }

    void Do(const TKV<K, TProtoMessage>& input, TOutput<TKV<K, TString>>& output) override
    {
        auto [cursor, _] = ReslicerMap_.try_emplace(input.Key(), Format_, MaxFrameSize_, MaxMessageCount_);
        cursor->second.Add(input.Value(), [&] (TString&& value) {
            output.Add({input.Key(), std::move(value)});
        });
    }

    void Finish(TOutput<TKV<K, TString>>& output) override
    {
        for (auto& cursor : ReslicerMap_) {
            cursor.second.Flush([&] (TString&& value) {
                output.Add({cursor.first, std::move(value)});
            });
        }
    }

private:
    NFraming::EFormat Format_ = NFraming::EFormat::Lenval;
    size_t MaxFrameSize_ = 2_MB;
    size_t MaxMessageCount_ = Max<size_t>();


    Y_SAVELOAD_DEFINE_OVERRIDE(Format_, MaxFrameSize_, MaxMessageCount_);

    THashMap<K, NPrivate::TFrameReslicer> ReslicerMap_;
};

class TPackFramedMessagesPerKeyTransform
{
public:
    TPackFramedMessagesPerKeyTransform(NFraming::EFormat format, size_t maxFrameSize, size_t maxMessageCount);

    TString GetName() const
    {
        return "PackFramedMessagesPerKey";
    }

    template <typename K, typename TProtoType>
        requires std::is_base_of_v<::google::protobuf::Message, TProtoType>
    auto ApplyTo(const TPCollection<TKV<K, TProtoType>>& pCollection) const
    {
        return pCollection |
            MakeParDo<TPackFramedMessagesPerKeyDoFn<K, TProtoType>>(Format_, MaxFrameSize_, MaxMessageCount_);
    }

private:
    NFraming::EFormat Format_;
    size_t MaxFrameSize_;
    size_t MaxMessageCount_;
};

TPackFramedMessagesPerKeyTransform PackFramedMessagesPerKeyParDo(
    NFraming::EFormat format,
    size_t maxFrameSize = 2_MB,
    size_t maxMessageCount = Max<size_t>());

////////////////////////////////////////////////////////////////////////////////

/// Transforms TKV::Key from some hash value to shard number (by range logic).
inline auto HashToShardByRangeTransform(ui64 shardCount, TString transformName = "HashToShardByRange") {
    return TGenericTransform(transformName, [=]<typename TValue>(const TPCollection<TKV<ui64, TValue>>& pCollection) {
        auto parDoF = [](ui64 shardCount, const NRoren::TKV<ui64, TValue>& msg) {
            return NRoren::TKV<ui64, TValue>{
                NBigRT::GetShardByRange(msg.Key(), shardCount),
                msg.Value(),
            };
        };
        return pCollection | NRoren::BindParDo(parDoF, shardCount);
    });
}

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <typename TValue>
class THashToShardByRangeParDo
    : public IDoFn<TKV<ui64, TValue>, TKV<ui64, TValue>>
{
public:
    using TTag = TTypeTag<IDestinationWriter>;
    using TInputRow = TKV<ui64, TValue>;
    using TOutputRow = TKV<ui64, TValue>;

    THashToShardByRangeParDo() = default;

    explicit THashToShardByRangeParDo(TTag id)
        : WriterId_(std::move(id))
    { }

    void Start(TOutput<TOutputRow>&) override
    {
        if (ShardsCount_ == 0) {
            const auto& compositeWriter = TBigRtExecutionContextOps::GetWriter(IDoFn<TInputRow, TOutputRow>::GetExecutionContext()->template As<IBigRtExecutionContext>());
            ShardsCount_ = compositeWriter->GetWriter(WriterId_).GetShardsCount();
        }
    }

    void Do(const TInputRow& input, TOutput<TOutputRow>& output) override
    {
        output.Add(TKV<ui64, TValue>{
            NBigRT::GetShardByRange(input.Key(), ShardsCount_),
            input.Value(),
        });
    }

private:
    TTag WriterId_ = TTag{"uninitialized"};

    ui64 ShardsCount_ = 0;

    Y_SAVELOAD_DEFINE_OVERRIDE(WriterId_, ShardsCount_);
};

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

inline auto HashToShardByRangeTransform(TString destinationName, TString transformName = "HashToShardByRange") {
    return TGenericTransform(transformName, [=]<typename TValue>(const TPCollection<TKV<ui64, TValue>>& pCollection) {
        auto tag = TTypeTag<NPrivate::IDestinationWriter>{destinationName};
        auto parDo = MakeParDo<NPrivate::THashToShardByRangeParDo<TValue>>(tag);
        return pCollection | parDo;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
