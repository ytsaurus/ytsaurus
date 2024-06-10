#include "builder.h"

#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/deprecated/json/writer.h>
#include <library/cpp/monlib/encode/encoder.h>
#include <library/cpp/monlib/metrics/metric_registry.h>


using namespace NMonitoring;

namespace NSolomon {
namespace {
    class TLegacyJsonConsumer final: public IMetricEncoder {
    public:
        TLegacyJsonConsumer(IOutputStream* os)
            : Writer_{os}
            , State_{ROOT}
        {
        }

        void OnStreamBegin() override {
            Writer_.OpenDocument();
            Writer_.OpenMetrics();
            State_ = SENSORS;
        }

        void OnStreamEnd() override {
            if (State_ == SENSORS) {
                Writer_.CloseMetrics();
            }

            Writer_.CloseDocument();
        }

        void OnCommonTime(TInstant) override {
            ythrow yexception() << "Not supported";
        }

        void OnMetricBegin(EMetricType kind) override {
            Writer_.OpenMetric();

            switch (kind) {
                case EMetricType::RATE:
                    Writer_.WriteModeDeriv();
                    break;
                case EMetricType::GAUGE:
                    break;
                default:
                    ythrow yexception() << "Only RATE and GAUGE types are supported in the legacy JSON format";
            };

            State_ = SENSOR;
        }

        void OnMetricEnd() override {
            Writer_.CloseMetric();
            State_ = SENSORS;
        }

        void OnLabelsBegin() override {
            if (State_ == SENSOR) {
                Writer_.OpenLabels();
            } else {
                Writer_.CloseMetrics();
                Writer_.OpenCommonLabels();
                State_ = COMMON_LABELS;
            }
        }

        void OnLabelsEnd() override {
            if (State_ == COMMON_LABELS) {
                Writer_.CloseCommonLabels();
                State_ = ROOT;
            } else {
                Writer_.CloseLabels();
            }
        }

        void OnLabel(TStringBuf name, TStringBuf value) override {
            if (State_ == COMMON_LABELS) {
                Writer_.WriteCommonLabel(name, value);
            } else {
                Writer_.WriteLabel(name, value);
            }
        }

        void OnDouble(TInstant time, double value) override {
            Writer_.WriteDoubleValue(value);
            if (time != TInstant::Zero()) {
                Writer_.WriteTs(time.Seconds());
            }
        }

        void OnInt64(TInstant time, i64 value) override {
            Writer_.WriteValue(value);
            if (time != TInstant::Zero()) {
                Writer_.WriteTs(time.Seconds());
            }
        }

        void OnUint64(TInstant time, ui64 value) override {
            Writer_.WriteValue(value);
            if (time != TInstant::Zero()) {
                Writer_.WriteTs(time.Seconds());
            }
        }

        void OnHistogram(TInstant, IHistogramSnapshotPtr) override {
            ythrow yexception() << "Histograms are not supported in the legacy JSON format";
        }

        void OnSummaryDouble(TInstant, ISummaryDoubleSnapshotPtr) override {
            ythrow yexception() << "Summary are not supported in the legacy JSON format";
        }

        void OnLogHistogram(TInstant, TLogHistogramSnapshotPtr) override {
            ythrow yexception() << "LogHistogram are not supported in the legacy JSON format";
        }

        void Close() override {
        }

    private:
        TDeprecatedJsonWriter Writer_;

        enum {
            ROOT,
            SENSORS,
            SENSOR,
            COMMON_LABELS,
        } State_;
    };
} // namespace
    class TSolomonJsonBuilder::TImpl {
    public:
        TImpl(TSolomonJsonBuilder::EFormat format)
            :  Format_{format}
        {
            Reset();
        }

        void Reset()  {
            switch (Format_) {
                case TSolomonJsonBuilder::EFormat::Json:
                    Consumer_ = BufferedEncoderJson(&Str_);
                    break;
                case TSolomonJsonBuilder::EFormat::Spack:
                    Consumer_ = EncoderSpackV1(&Str_, ETimePrecision::SECONDS, ECompression::ZSTD);
                    break;
                case TSolomonJsonBuilder::EFormat::LegacyJson:
                    Consumer_.Reset(new TLegacyJsonConsumer(&Str_));
                    break;
            }

            Consumer_->OnStreamBegin();
            Str_.Clear();
        }

        TString Build() {
            Consumer_->OnStreamEnd();
            Consumer_->Close();
            Str_.Flush();
            const auto result = std::move(Str_.Str());
            Reset();
            return result;
        }

        template <typename T>
        void AddDerivSensor(const TSolomonJsonBuilder::TSensorPath& labels, T value) {
            WriteSensor(*Consumer_, EMetricType::RATE, labels, value);
        }

        template <typename T>
        void AddDerivSensor(const TSolomonJsonBuilder::TSensorPath& labels, TInstant timestamp, T value) {
            WriteSensor(*Consumer_, EMetricType::RATE, labels, value, timestamp);
        }

        template <typename T>
        void AddSensor(const TSolomonJsonBuilder::TSensorPath& labels, T value) {
            WriteSensor(*Consumer_, EMetricType::GAUGE, labels, value);
        }

        template <typename T>
        void AddSensor(const TSolomonJsonBuilder::TSensorPath& labels, TInstant timestamp, T value) {
            WriteSensor(*Consumer_, EMetricType::GAUGE, labels, value, timestamp);
        }

        void WriteCommonLabels(const TSolomonJsonBuilder::TSensorPath& labels) {
            Consumer_->OnLabelsBegin();
            for (auto&& [name, value] : labels) {
                Consumer_->OnLabel(name, value);
            }

            Consumer_->OnLabelsEnd();
        }

        TSolomonJsonBuilder::EFormat Format() const {
            return Format_;
        }

    private:
        template <typename T>
        void WriteSensor(IMetricConsumer& c, EMetricType kind, const TSolomonJsonBuilder::TSensorPath& labels, T value, TInstant ts = TInstant::Zero()) {
            c.OnMetricBegin(kind);
            c.OnLabelsBegin();
            for (auto&& [name, value] : labels) {
                c.OnLabel(name, value);
            }

            c.OnLabelsEnd();

            switch (kind) {
                case EMetricType::RATE:
                    c.OnUint64(ts, value);
                    break;
                case EMetricType::GAUGE:
                    c.OnDouble(ts, value);
                    break;
                default:
                    ythrow yexception() << "Only RATE and GAUGE sensors are allowed";
            }

            c.OnMetricEnd();
        }

    private:
        TSolomonJsonBuilder::EFormat Format_;
        TStringStream Str_;
        IMetricEncoderPtr Consumer_;
    };

    template <typename T>
    void TSolomonJsonBuilder::AddDerivSensor(const TSolomonJsonBuilder::TSensorPath& labels, T value) {
        Impl_->AddDerivSensor(labels, value);
    }

    template <typename T>
    void TSolomonJsonBuilder::AddDerivSensor(const TSolomonJsonBuilder::TSensorPath& labels, TInstant timestamp, T value) {
        Impl_->AddDerivSensor(labels, timestamp, value);
    }

    template <typename T>
    void TSolomonJsonBuilder::AddSensor(const TSolomonJsonBuilder::TSensorPath& labels, TInstant timestamp, T value) {
        Impl_->AddSensor(labels, timestamp, value);
    }

    template <typename T>
    void TSolomonJsonBuilder::AddSensor(const TSolomonJsonBuilder::TSensorPath& labels, T value) {
        Impl_->AddSensor(labels, value);
    }

    TSolomonJsonBuilder::TSolomonJsonBuilder(EFormat format) {
        Impl_.Reset(new TImpl{format});
    }

    TSolomonJsonBuilder::~TSolomonJsonBuilder() = default;

    TString TSolomonJsonBuilder::GetJsonValue() {
        return Impl_->Build();
    }

    void TSolomonJsonBuilder::Reset() {
        Impl_->Reset();
    }

    void TSolomonJsonBuilder::WriteCommonLabels(const TSolomonJsonBuilder::TSensorPath& labels) {
        Impl_->WriteCommonLabels(labels);
    }

    TSolomonJsonBuilder::EFormat TSolomonJsonBuilder::Format() const {
        return Impl_->Format();
    }

#define Y_INSTANTIATE_TEMPLATE(Func) \
    template void TSolomonJsonBuilder::Func<double>(const TSolomonJsonBuilder::TSensorPath& labels, double value); \
    template void TSolomonJsonBuilder::Func<double>(const TSolomonJsonBuilder::TSensorPath& labels, TInstant, double value); \
    template void TSolomonJsonBuilder::Func<long>(const TSolomonJsonBuilder::TSensorPath& labels, long value); \
    template void TSolomonJsonBuilder::Func<long>(const TSolomonJsonBuilder::TSensorPath& labels, TInstant, long value); \
    template void TSolomonJsonBuilder::Func<unsigned long>(const TSolomonJsonBuilder::TSensorPath& labels, unsigned long value); \
    template void TSolomonJsonBuilder::Func<unsigned long>(const TSolomonJsonBuilder::TSensorPath& labels, TInstant, unsigned long value);

    Y_INSTANTIATE_TEMPLATE(AddSensor)
    Y_INSTANTIATE_TEMPLATE(AddDerivSensor)
} // namespace NSolomon
