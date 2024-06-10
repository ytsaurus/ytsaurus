#include "transforms.h"

#include <yt/cpp/roren/interface/transforms.h>
#include <yt/cpp/roren/library/logger/logger.h>

#include <bigrt/lib/queue/message_batch/message_batch.h>
#include <bigrt/lib/utility/profiling/fiber_vcpu_time.h>
#include <bigrt/lib/processing/resharder/profiling/profiler.h>

#include <library/cpp/logger/global/global.h>

#include <util/datetime/cputimer.h>
#include <util/string/escape.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>
#include <library/cpp/yt/memory/serialize.h>
#include <library/cpp/yt/small_containers/compact_vector.h>

#include <yt/yt/library/profiling/tag.h>

#include <util/generic/xrange.h>
#include <util/ysaveload.h>

#include <type_traits>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TUnpackMessagesFn : public IDoFn<NBigRT::TMessageBatch, TString>
{
public:
    using TMetrics = TUnpackMessagesOptions::TMetrics;

    TUnpackMessagesFn() = default;

    TUnpackMessagesFn(TUnpackMessagesOptions options)
        : Options_(std::move(options))
    {
    }

    void Start(TOutput<TString>&) override
    {
        MetricIncrements_ = {};
        if (!Metrics_) {
            Metrics_ = Options_.MetricsFactory(GetExecutionContext()->GetProfiler().WithTags(Options_.ProfilerTags));
        }
    }

    void Do(const NBigRT::TMessageBatch& messageBatch, TOutput<TString>& output) override
    {
        for (auto message: messageBatch.Messages) {
            ++MetricIncrements_.InputCount;
            if (!message.Unpack()) {
                ++MetricIncrements_.UnpackErrorCount;
                continue;
            }

            ++MetricIncrements_.UnpackOkCount;
            output.Add(message.UnpackedData());
        }
    }

    void Finish(TOutput<TString>&) override
    {
        MetricIncrements_.Flush(*Metrics_);
    }

private:
    struct TMetricIncrements
    {
        ui64 InputCount = 0;
        ui64 UnpackErrorCount = 0;
        ui64 UnpackOkCount = 0;

        void Flush(const TMetrics& metrics)
        {
            metrics.InputCount.Increment(InputCount);
            metrics.UnpackOkCount.Increment(UnpackOkCount);
            metrics.UnpackErrorCount.Increment(UnpackErrorCount);
        }
    };

private:
    TUnpackMessagesOptions Options_;

    Y_SAVELOAD_DEFINE_OVERRIDE(Options_);

    TMetricIncrements MetricIncrements_;
    TMaybe<TMetrics> Metrics_;
};

TParDoTransform<NBigRT::TMessageBatch, TString> UnpackMessagesParDo(TUnpackMessagesOptions options)
{
    return MakeParDo<TUnpackMessagesFn>(std::move(options));
}

TPackFramedMessagesTransform PackFramedMessagesParDo(NFraming::EFormat format, size_t maxSize)
{
    return TPackFramedMessagesTransform(format, maxSize);
}

////////////////////////////////////////////////////////////////////////////////

TPackFramedMessagesTransform::TPackFramedMessagesTransform(NFraming::EFormat format, size_t maxSize)
    : Format_(std::move(format)), MaxSize_(maxSize)
{ }

////////////////////////////////////////////////////////////////////////////////

TPackFramedMessagesPerKeyTransform::TPackFramedMessagesPerKeyTransform(
    NFraming::EFormat format,
    size_t maxFrameSize,
    size_t maxMessageCount)
    : Format_(format)
    , MaxFrameSize_(maxFrameSize)
    , MaxMessageCount_(maxMessageCount)
{ }

TPackFramedMessagesPerKeyTransform PackFramedMessagesPerKeyParDo(
    NFraming::EFormat format,
    size_t maxFrameSize,
    size_t maxMessageCount)
{
    return {format, maxFrameSize, maxMessageCount};
}

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

void TStringReslicer::Reset()
{
    Buffer_.clear();
}

void TStringReslicer::Add(const TString& value, size_t maxSize, const TConsumer& consumer)
{
    auto newSize = value.size() + Buffer_.size();

    if (newSize < maxSize) {
        Buffer_ += value;
    } else if (Buffer_.empty()) {
        if (value.size() > 0) {
            consumer(value);
        }
    } else if (newSize == maxSize) {
        Buffer_ += value;
        consumer(Buffer_);
        Buffer_.clear();
    } else {
        Y_ASSERT(!Buffer_.empty() && newSize > maxSize);
        consumer(Buffer_);
        if (value.size() < maxSize) {
            Buffer_.assign(value.begin(), value.end());
        } else {
            consumer(value);
            Buffer_.clear();
        }
    }
}

void TStringReslicer::Flush(const TConsumer& consumer)
{
    if (!Buffer_.empty()) {
        consumer(Buffer_);
    }
    Buffer_.clear();
}

TFrameReslicer::TFrameReslicer(NFraming::EFormat format, size_t maxFrameSize, size_t maxMessageCount)
    : Packer_(format, Stream_)
    , MaxFrameSize_(maxFrameSize)
    , MaxMessageCount_(maxMessageCount)
{ }

void TFrameReslicer::Add(const google::protobuf::Message& message, TConsumer consumer)
{
    const size_t sizeAfter = Stream_.Size() + Packer_.GetFrameSize(message.ByteSize());
    if (0 != MessageCount_ && sizeAfter > MaxFrameSize_) {
        DoFlush(consumer);
    }
    Packer_.Add(message, true /*useCachedSize=*/);
    if (++MessageCount_ >= MaxMessageCount_) {
        DoFlush(consumer);
    }
}

void TFrameReslicer::Flush(TConsumer consumer)
{
    Packer_.Flush();
    if (!Stream_.Empty()) {
        DoFlush(consumer);
    }
}

void TFrameReslicer::DoFlush(TConsumer& consumer)
{
    TString tmp;
    tmp.swap(Stream_.Str());
    consumer(std::move(tmp));
    Stream_.Clear();
    MessageCount_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
