#pragma once

#include <library/cpp/json/json_writer.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/nth_elements/nth_elements.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/function.h>
#include <util/generic/store_policy.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/variant.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/system/event.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>
#include <util/thread/lfqueue.h>
#include <util/thread/factory.h>


namespace NSFStats {
    namespace NPrivate {

        template <class T>
        struct TMetricTypeTraits {
        };

        template <>
        struct TMetricTypeTraits<ui64> {
            using TValue = ui64;
        };
        template <>
        struct TMetricTypeTraits<i64> {
            using TValue = ui64;
        };
        template <>
        struct TMetricTypeTraits<ui32> {
            using TValue = ui64;
        };
        template <>
        struct TMetricTypeTraits<i32> {
            using TValue = ui64;
        };

        template <>
        struct TMetricTypeTraits<double> {
            using TValue = double;
        };
        template <>
        struct TMetricTypeTraits<float> {
            using TValue = double;
        };

        template <class T>
        class TAutoLockFreeLimitedBlockingQueue {
        private:
            struct TCounter : TAtomicCounter {
                inline void IncCount(const T* const&) {
                    Inc();
                }

                inline void DecCount(const T* const&) {
                    Dec();
                }
            };
        public:
            typedef THolder<T> TRef;

            explicit TAutoLockFreeLimitedBlockingQueue(i64 limit)
                : Limit_(limit)
            {
                Y_ABORT_UNLESS(Limit_ > 0);
            }

            bool Schedule(TRef& ref) {
                if (Q_.GetCounter().Val() >= Limit_) {
                    return false;
                }
                Q_.Enqueue(ref);
                if (C_.Val()) {
                    Write_.Signal();
                }
                return true;
            }

            void ForceSchedule(TRef ref) {
                Q_.Enqueue(ref);
                if (C_.Val()) {
                    Write_.Signal();
                }
            }

            TRef Next() {
                TRef ret;
                C_.Inc();
                while (!Q_.Dequeue(&ret)) {
                    Write_.Wait();
                }
                C_.Dec();
                // to notify other consumers
                if (Q_.GetCounter().Val() && C_.Val()) {
                    Write_.Signal();
                }
                return ret;
            }
        private:
            i64 Limit_;
            TAutoEvent Write_;
            TAtomicCounter C_;
            TAutoLockFreeQueue<T, TCounter> Q_;
        };
    }

    // To implement your own TMetric please take a look at TSumMetric/TLastMetric/TPercentileMetric classes below
    class TBasicMetric {
    public:
        using TFunc = std::function<void(const TString&, const std::variant<ui64, double>&, bool)>;

        TBasicMetric(const TString& name);
        virtual void Visit(TFunc&& func) noexcept = 0;
        virtual ~TBasicMetric() = default;
        const TString& Name() const;
    private:
        const TString Name_;
    };
    using TMetricPtr = THolder<TBasicMetric>;

    template <class T>
    class TSumMetric;

    class TStats;

    class TStatsState {
        friend class TStats;
    public:
        using TStateFunc = std::function<void(TStatsState&)>;
        using TFunc = std::function<void()>;
        using TFuncQueue = NPrivate::TAutoLockFreeLimitedBlockingQueue<TFunc>;

        template <class TDerived>
        class TContextExtender {
        public:
            template <class T>
            inline void Inc(const TString& name, T value) {
                using TInc = typename NPrivate::TMetricTypeTraits<T>::TValue;
                static_cast<TDerived*>(this)->template Get<TSumMetric<TInc>>(name).Inc(value);
            }

            template <class T>
            inline void Dec(const TString& name, T value) {
                using TInc = typename NPrivate::TMetricTypeTraits<T>::TValue;
                static_cast<TDerived*>(this)->template Get<TSumMetric<TInc>>(name).Dec(value);
            }

            template <class TContainer>
            inline void IncAll(TContainer&& container) {
                for (const auto& c : container) {
                    Inc(c.first, c.second);
                }
            }

            template <class TContainer>
            inline void DecAll(TContainer&& container) {
                for (const auto& c : container) {
                    Dec(c.first, c.second);
                }
            }
        };

        class TContext : public TContextExtender<TContext> {
            friend class TStats;
            friend class TStatsState;
        public:
            template <class TMetric>
            class TBasicProxy {
            public:
                TBasicProxy(const TString& name, TContext& ctx)
                    : Name_(name)
                    , Ctx_(&ctx)
                {
                }

            protected:
                template <class TUpdate>
                void AddFunc(TUpdate&& update) {
                    Ctx_->AddFunc([update, name = Name_](TStatsState& s) {
                        auto* metric = dynamic_cast<TMetric*>(s.Metric<TMetric>(name).Get());
                        Y_ENSURE(metric);
                        update(metric);
                    });
                }
            private:
                TString Name_;
                TContext* Ctx_;
            };

            inline TContext(const TAtomicSharedPtr<TStatsState>& s) noexcept
                : State_(s)
                , F_(new TDeque<TStateFunc>)
            {
            }
            TContext(const TStats& s) noexcept;

            inline ~TContext() {
                State_->Apply(*this);
            }

            inline void Flush() {
                State_->Apply(*this);
                F_.Reset(new TDeque<TStateFunc>);
            }

            template <class TMetric, class TProxy = typename TMetric::TProxy>
            inline TProxy Get(const TString& name) {
                return {name, *this};
            }

            template <class TMetric>
            inline typename TMetric::template TProxy<TBasicProxy<TMetric>> Get(const TString& name) {
                return {name, *this};
            }

            const TAtomicSharedPtr<TStatsState>& GetStatsState() {
                return State_;
            }

        private:
            inline void AddFunc(TStateFunc func) {
                with_lock (L_) {
                    F_->emplace_back(func);
                }
            }

        private:
            TAtomicSharedPtr<TStatsState> State_;
            TAtomicSharedPtr<TDeque<TStateFunc>> F_;
            TAdaptiveLock L_;
        };

        TStatsState() = default;
        ~TStatsState() = default;
    private:

        void Apply(TContext& ctx);

        template <class T>
        inline TMetricPtr& Metric(const TString& name) {
            auto& result = M_[name];
            if (!result) {
                result.Reset(new T(name));
            }

            return result;
        }

        TFuncQueue* F_ = nullptr;
        THolder<TFuncQueue> QueueHolder_;
        TAdaptiveLock ML_;
        THashMap<TString, TMetricPtr> M_;
        std::atomic<ui64> QueueOverflowCounter_;
    };
    using TStatsStatePtr = TAtomicSharedPtr<TStatsState>;

    class TStats {
    public:
        using TContext = TStatsState::TContext;

        template <class TDerived>
        using TContextExtender = TStatsState::TContextExtender<TDerived>;

        TStats();
        TStats(TStats&&) = default;
        explicit TStats(ui64 limit, bool reportQueueOverflows = false);
        TStats(TStatsState::TFuncQueue* commonQueue);
        ~TStats();

        const TStatsStatePtr& GetState() const;

        template <class F>
        inline void Out(F&& f) {
            with_lock (State_->ML_) {
                for (const auto& x : State_->M_) {
                    x.second->Visit([&f](const TString& m, const std::variant<ui64, double>& v, bool d) {
                        std::visit(
                            [&f, &m, d](const auto& vv) {
                                Call(f, m, vv, d);
                            },
                            v
                        );
                    });
                }
                if (ReportQueueOverflows_) {
                    Call(f, "/safe_stats/queue_overflow", State_->QueueOverflowCounter_.load(), /* derived */ true);
                }
            }
        }

    private:
        template <class F, class V>
        static std::enable_if_t<std::is_invocable_v<F, const TString&, const V&, bool>, void> Call(F&& f, const TString& m, const V& v, bool d) {
            f(m, v, d);
        }

        // for back-compatibility
        template <class F, class V>
        static std::enable_if_t<!std::is_invocable_v<F, const TString&, const V&, bool> && std::is_invocable_v<F, const TString&, const V&>, void> Call(F&& f, const TString& m, const V& v, bool d) {
            Y_UNUSED(d);
            f(m, v);
        }

        TStatsStatePtr State_;
        THolder<IThreadFactory::IThread> T_;
        bool ReportQueueOverflows_ = false;
    };

    class TCommonStats {
    public:
        TCommonStats(ui64 limit);
        ~TCommonStats();

        TStats CreateStats();
        void Finish();

    private:
        THolder<TStatsState::TFuncQueue> Q_;
        THolder<IThreadFactory::IThread> T_;
    };

    template <class T, ui32 Alpha1000000, ui64 MinDeltaUs, class TTime = TInstant>
    class TExpMovingAverage : public TBasicMetric {
        constexpr static double Alpha = Alpha1000000 / 1000000.;
        constexpr static TDuration MinDelta = TDuration::MicroSeconds(MinDeltaUs);
    public:
        TExpMovingAverage(const TString& name) : TBasicMetric(name) {
            Last_ = TTime::Now();
        }
        using TClass = TExpMovingAverage<T, Alpha1000000, MinDeltaUs, TTime>;
        using TBasicMetric::TBasicMetric;
        using TBasicProxy = TStats::TContext::TBasicProxy<TClass>;

        template <class TBase = TBasicProxy>
        class TProxy : public TBase {
        public:
            using TBase::TBase;

            void Add(T v) {
                Sum_ += v;
            }

            ~TProxy() {
                this->AddFunc([sum = Sum_](TClass* m) {
                    m->Add(sum);
                });
            }

        private:
            T Sum_ = 0;
        };

        void Add(T v) {
            Uncounted_ += v;
        }

        void Visit(TFunc&& func) noexcept override {
            const auto now = TTime::Now();
            const auto delta = (now - Last_);
            if (delta < MinDelta) {
                func(Name(), Metric_, false);
                return;
            }

            Last_ = now;

            const double rate = Uncounted_ / delta.SecondsFloat();
            Uncounted_ = 0;

            Metric_ += Alpha * (rate - Metric_);

            func(Name(), Metric_, false);
        }

    private:
        T Uncounted_ = 0;
        double Metric_ = 0;
        TInstant Last_;
    };

    template <class T>
    class TSumMetric : public TBasicMetric {
    public:
        using TBasicMetric::TBasicMetric;
        using TBasicProxy = TStats::TContext::TBasicProxy<TSumMetric<T>>;

        template <class TBase = TBasicProxy>
        class TProxy : public TBase {
        public:
            using TBase::TBase;

            void Inc(T v) {
                ToInc_ += v;
            }
            void Dec(T v) {
                ToDec_ += v;
            }
            ~TProxy() {
                this->AddFunc([toInc = ToInc_, toDec = ToDec_](TSumMetric<T>* m) {
                    m->Inc(toInc);
                    m->Dec(toDec);
                });
            }
        private:
            T ToInc_ = 0;
            T ToDec_ = 0;
        };

        void Inc(T v) noexcept {
            V_ += v;
        }
        void Dec(T v) noexcept {
            V_ -= v;
        }
        void Visit(TFunc&& func) noexcept override {
            func(this->Name(), V_, true);
        }
    private:
        T V_ = 0;
    };

    template <class T>
    class TLastMetric : public TBasicMetric {
    public:
        using TBasicMetric::TBasicMetric;
        using TBasicProxy = TStats::TContext::TBasicProxy<TLastMetric<T>>;

        template <class TBase = TBasicProxy>
        class TProxy : public TBase {
        public:
            using TBase::TBase;

            void Set(T v) {
                V_ = v;
            }
            void Reset() {
                V_ = 0;
            }
            ~TProxy() {
                if (V_.Defined()) {
                    this->AddFunc([v = *V_](TLastMetric<T>* m) {
                        m->Set(v);
                    });
                }
            }
        private:
            TMaybe<T> V_;
        };

        void Set(T v) {
            V_ = v;
        }

        void Visit(TFunc&& func) noexcept override {
            func(this->Name(), V_, false);
        }
    private:
        T V_ = 0;
    };

    // Publishes sum of all followed atomics
    template <class T>
    class TAtomicMetric : public TBasicMetric {
    public:
        using TBasicMetric::TBasicMetric;
        using TBasicProxy = TStats::TContext::TBasicProxy<TLastMetric<T>>;

        template <class TBase = TBasicProxy>
        class TProxy : public TBase {
        public:
            using TBase::TBase;

            void Follow(std::weak_ptr<std::atomic<T>> v) {
                this->AddFunc([v](TAtomicMetric<T>* m) {
                    m->Follow(v);
                });
            }
        };

        void Follow(std::weak_ptr<std::atomic<T>> v) {
            Values_.push_back(std::move(v));
        }

        void Visit(TFunc&& func) noexcept override {
            T sum = {0};
            for (auto it = Values_.begin(); it != Values_.end();) {
                auto v = it->lock();
                if (!v) {
                    std::swap(*it, Values_.back());
                    Values_.pop_back();
                    continue;
                }
                sum += v->load();
                ++it;
            }
            func(this->Name(), sum, false);
        }
    private:
        TVector<std::weak_ptr<std::atomic<T>>> Values_ = {};
    };


    const char SOLOMON_LABEL_DELIMITER = '\0';
    const char SOLOMON_VALUE_DELIMITER = '\1';

    class TSolomonContext : public TStats::TContextExtender<TSolomonContext> {
    public:
        struct TLabel {
            TString Name;
            TString Value;

            TLabel()
                : Name()
                , Value()
            {
            }

            TLabel(const TString& name, const TString& value)
                : Name(name)
                , Value(value)
            {
            }
        };

        static TVector<TLabel> MergeLabels(TVector<TLabel> labels) {
            std::stable_sort(labels.begin(), labels.end(), [](const auto& l, const auto& r) {
                return l.Name < r.Name;
            });
            size_t target = 0;
            size_t current = 0;
            while (current < labels.size()) {
                if (current + 1 < labels.size() && labels[current].Name == labels[current + 1].Name) {
                    ++current;
                } else {
                    labels[target] = std::move(labels[current]);
                    ++target;
                    ++current;
                }
            }
            labels.resize(target);
            return labels;
        }

        static TString BuildLabelString(const TVector<TLabel>& labels) {
            TStringBuilder builder;
            for (const auto& l : labels) {
                Y_ENSURE(l.Name.find(SOLOMON_LABEL_DELIMITER) == TString::npos);
                Y_ENSURE(l.Value.find(SOLOMON_LABEL_DELIMITER) == TString::npos);
                Y_ENSURE(l.Name.find(SOLOMON_VALUE_DELIMITER) == TString::npos);
                Y_ENSURE(l.Value.find(SOLOMON_VALUE_DELIMITER) == TString::npos);
                builder << l.Name << SOLOMON_VALUE_DELIMITER << l.Value << SOLOMON_LABEL_DELIMITER;
            }
            return builder;
        }

        static TString BuildPrefixedName(TStringBuf prefix, TStringBuf name) {
            if (prefix.empty()) {
                return TStringBuilder{} << name;
            }
            if (name.empty()) {
                return TStringBuilder{} << prefix;
            }
            return TStringBuilder{} << prefix << "." << name;
        }

        static TString BuildMetricName(TStringBuf labelString, TStringBuf name) {
            return TStringBuilder{} << labelString << name;
        }

        static TString BuildMetricName(const TVector<TLabel>& labels, TStringBuf name) {
            return TStringBuilder{} << BuildLabelString(MergeLabels(labels)) << name;
        }

        inline TSolomonContext(TStats::TContext& ctx, const TVector<TLabel>& labels = {}, const TString& prefix = "")
            : Ctx_(&ctx)
            , Labels_(MergeLabels(labels))
            , LabelString_(BuildLabelString(Labels_))
            , Prefix_(prefix)
        {
        }

        inline TSolomonContext(const TSolomonContext& context, const TVector<TLabel>& labels, const TString& prefix = "")
            : TSolomonContext(
                *context.Ctx_,
                [&]() {
                    TVector<TLabel> l = context.Labels_;
                    l.insert(l.end(), labels.begin(), labels.end());
                    return l;
                } (),
                BuildPrefixedName(context.Prefix_, prefix)
            )
        {
            CtxHolder_ = context.CtxHolder_;
        }

        inline TSolomonContext(const TStatsStatePtr& statsState, const TVector<TLabel>& labels, const TString& prefix = "")
            : TSolomonContext(MakeAtomicShared<TStats::TContext>(statsState), labels, prefix)
        {
        }

        inline TSolomonContext(const TStats& stats, TVector<TLabel> labels = {}, const TString& prefix = "")
            : TSolomonContext(stats.GetState(), labels, prefix)
        {
        }

        //! Creates a copy of TSolomonContext with new TContext. May be useful for creating short-living solomon contexts from long-living one
        [[nodiscard]] TSolomonContext Detached() && {
            CtxHolder_ = MakeAtomicShared<TStats::TContext>(Ctx_->GetStatsState());
            Ctx_ = CtxHolder_.Get();
            return *this;
        }

        [[nodiscard]] TSolomonContext Detached() const & {
            return TSolomonContext{Ctx_->GetStatsState(), Labels_, Prefix_};
        }

        inline void Flush() {
            Ctx_->Flush();
        }

        template <class TMetric>
        inline auto Get(const TString& name) {
            return Ctx_->Get<TMetric>(BuildMetricName(LabelString_, BuildPrefixedName(Prefix_, name)));
        }

        TVector<TLabel> ExportLabels() const {
            return Labels_;
        }

        // WARNING: access by reference. Use only for short-time calls when lifetime of the
        // context itself are guaranteed to be bigger. Otherwise see ExportLabels method above.
        const TVector<TLabel>& GetLabels() const {
            return Labels_;
        }

    protected:
        inline TSolomonContext(TAtomicSharedPtr<TStats::TContext>&& ctx, TVector<TLabel> labels = {}, const TString& prefix = "")
            : TSolomonContext(*ctx.Get(), labels, prefix)
        {
            CtxHolder_ = std::move(ctx);
        }

    private:
        friend class TSolomonMultiContext;

        TAtomicSharedPtr<TStats::TContext> CtxHolder_; // stores context if TSolomonContext owns TContext
        TStats::TContext* Ctx_; // == CtxHolder_.Get() if CtxHolder_ is not empty
        TVector<TLabel> Labels_;
        TString LabelString_;
        TString Prefix_;
    };

    class TSolomonMultiContext : public TStats::TContextExtender<TSolomonMultiContext>, TMoveOnly {
    public:
        template <class TMetric>
        class TBasicProxy {
            using TContextBasicProxy = TStats::TContext::TBasicProxy<TMetric>;

            struct TPipeProxy : TContextBasicProxy {
                using TContextBasicProxy::TContextBasicProxy;

                template <class TUpdate>
                void AddFunc(TUpdate&& update) {
                    TContextBasicProxy::AddFunc(std::forward<TUpdate>(update));
                }
            };

        public:
            TBasicProxy(const TString& name, TSolomonMultiContext& ctx)
                : Name_(name)
                , MultiContext_(&ctx)
            {
            }

        protected:
            template <class TUpdate>
            void AddFunc(TUpdate&& update) {
                for (TSolomonContext& ctx : MultiContext_->Contexts_) {
                    TString name = TSolomonContext::BuildMetricName(
                        ctx.LabelString_,
                        TSolomonContext::BuildPrefixedName(ctx.Prefix_, Name_));
                    TPipeProxy{std::move(name), *ctx.Ctx_}.AddFunc(update);
                }
            }

        private:
            TString Name_;
            TSolomonMultiContext* MultiContext_;
        };

        TSolomonMultiContext(const TSolomonContext& context, const TVector<TVector<TSolomonContext::TLabel>>& labels = {{}})
        {
            for (const auto& l : labels) {
                Contexts_.emplace_back(context, l);
            }
        }

        TSolomonMultiContext(const TSolomonMultiContext& multiContext, const TVector<TVector<TSolomonContext::TLabel>>& labels = {{}})
        {
            for (const auto& ctx : multiContext.Contexts_) {
                for (const auto& l : labels) {
                    Contexts_.emplace_back(ctx, l);
                }
            }
        }

        TSolomonMultiContext(TVector<TSolomonContext> contexts)
            : Contexts_(std::move(contexts))
        {
        }

        [[nodiscard]] TSolomonMultiContext Detached() const & {
            TVector<TSolomonContext> detachedContexts;
            for (const auto& ctx : Contexts_) {
                detachedContexts.push_back(ctx.Detached());
            }
            return TSolomonMultiContext(std::move(detachedContexts));
        }

        template <class... TContexts>
        TSolomonMultiContext(const TSolomonMultiContext& ctx, const TSolomonMultiContext& ctx2, TContexts&&... ctxs)
            : TSolomonMultiContext(ctx2, std::forward<TContexts>(ctxs)...)
        {
            Contexts_.insert(Contexts_.end(), ctx.Contexts_.begin(), ctx.Contexts_.end());
        }

        template <class TMetric>
        inline typename TMetric::template TProxy<TBasicProxy<TMetric>> Get(const TString& name) {
            return {name, *this};
        }

    protected:
        TVector<TSolomonContext> Contexts_;
    };

    struct TSolomonOut {
        NJson::TJsonWriter Writer;
        TVector<std::pair<TString, TString>> CommonLabels;

        TSolomonOut(IOutputStream& out, TVector<std::pair<TString, TString>> commonLabels = {})
            : Writer(&out, false)
            , CommonLabels(std::move(commonLabels))
        {
            Writer.OpenMap();
            Writer.OpenArray("sensors");
        }

        ~TSolomonOut() {
            try {
                Writer.CloseArray();
                Writer.CloseMap();
            } catch (...) {
                Cerr << CurrentExceptionMessage() << "\n";
            }
        }

        template <class T>
        void operator()(TStringBuf k, T v, bool deriv = true) {
            Writer.OpenMap();
            Writer.OpenMap("labels");
            for (const auto& [name, value] : CommonLabels) {
                Writer.Write(name, value);
            }
            while (k.length() > 0) {
                TStringBuf label = k.NextTok(SOLOMON_LABEL_DELIMITER);
                if (label.empty()) {
                    continue;
                }
                auto delim = label.find(SOLOMON_VALUE_DELIMITER);
                if (delim == TStringBuf::npos) {
                    Writer.Write("sensor", label);
                } else {
                    Writer.Write(label.Head(delim), label.Tail(delim + 1));
                }
            }
            Writer.CloseMap();
            if (deriv) {
                Writer.Write("mode", "deriv");
            }
            Writer.Write("value", v);
            Writer.CloseMap();
        }
    };

    struct TSolomonOutSpack {
        NMonitoring::IMetricEncoderPtr Encoder;
        TInstant Ts;
        TVector<std::pair<TString, TString>> CommonLabels;

        TSolomonOutSpack(IOutputStream& out, TInstant ts, TVector<std::pair<TString, TString>> commonLabels = {})
            : Ts(ts)
            , CommonLabels(std::move(commonLabels))
        {
            Encoder = NMonitoring::EncoderSpackV1(
                &out,
                NMonitoring::ETimePrecision::SECONDS,
                NMonitoring::ECompression::ZSTD
            );
            Encoder->OnStreamBegin();
        }

        ~TSolomonOutSpack() {
            try {
                Encoder->OnStreamEnd();
                Encoder->Close();
            } catch (...) {
                Cerr << CurrentExceptionMessage() << "\n";
            }
        }

        template <class T>
        void operator()(TStringBuf k, T v, bool deriv = true) {
            if (deriv) {
                Encoder->OnMetricBegin(NMonitoring::EMetricType::RATE);
            } else {
                Encoder->OnMetricBegin(NMonitoring::EMetricType::GAUGE);
            }

            Encoder->OnLabelsBegin();
            for (const auto& [name, value] : CommonLabels) {
                Encoder->OnLabel(name, value);
            }
            while (k.length() > 0) {
                TStringBuf label = k.NextTok(SOLOMON_LABEL_DELIMITER);
                if (label.empty()) {
                    continue;
                }

                auto delim = label.find(SOLOMON_VALUE_DELIMITER);
                if (delim == TStringBuf::npos) {
                    Encoder->OnLabel("sensor", label);
                } else {
                    Encoder->OnLabel(label.Head(delim), label.Tail(delim + 1));
                }
            }
            Encoder->OnLabelsEnd();

            if (deriv) {
                Encoder->OnUint64(TInstant::Zero(), static_cast<ui64>(v));
            } else {
                Encoder->OnDouble(Ts, static_cast<double>(v));
            }
            Encoder->OnMetricEnd();
        }
    };

    struct TThreshold {
        ui64 Threshold;
        TString Name;
        ui64 Count;

        TThreshold(ui64 threshold, const TString& name)
            : Threshold(threshold)
            , Name(name)
            , Count(0)
        {
        }
        TThreshold(ui64 threshold)
            : Threshold(threshold)
            , Name(ToString(threshold))
            , Count(0)
        {
        }
    };

    template <class TDerived, ui64... thresholds>
    class TBasicThresholdMetric : public TBasicMetric {
    public:
        template <class... TArgs>
        TBasicThresholdMetric(TArgs&&... args)
            : TBasicMetric(std::forward<TArgs>(args)...)
            , Values_({TThreshold{thresholds}...})
        {
            Values_.emplace_back(static_cast<ui64>(-1), "inf");
            SortBy(Values_, [](const TThreshold& t) {
                return t.Threshold;
            });
        }

        using TBasicProxy = TStats::TContext::TBasicProxy<TDerived>;

        template <class TBase = TBasicProxy>
        class TProxy : public TBase {
        public:
            using TBase::TBase;

            template <class T>
            void Add(T x) {
                this->AddFunc([x](auto* m) {
                    m->Add(x);
                });
            }
            void Reset() {
                this->Reset();
            }
        };

        template <class T>
        void Add(T x) {
            auto it = LowerBoundBy(Values_.begin(), Values_.end(), x, [](const TThreshold& t) {
                return t.Threshold;
            });
            if (it != Values_.end()) {
                ++it->Count;
            }
        }

        void Visit(TFunc&& func) noexcept override {
            for (const auto& v : Values_) {
                func(GetFullName(v.Name), v.Count, true);
            }
        }
    private:
        virtual TString GetFullName(const TString& name) const = 0;

        TVector<TThreshold> Values_;
    };

    template <class TDerived, ui64... thresholds>
    class TBasicLowerThresholdMetric : public TBasicMetric {
    public:
        template <class... TArgs>
        TBasicLowerThresholdMetric(TArgs&&... args)
            : TBasicMetric(std::forward<TArgs>(args)...)
            , Values_({TThreshold{thresholds}...})
        {
            Values_.emplace_back(static_cast<ui64>(-1), "inf");
            Sort(Values_, [](const auto& l, const auto& r) {
                return l.Threshold > r.Threshold;
            });
        }

        using TBasicProxy = TStats::TContext::TBasicProxy<TDerived>;

        template <class TBase = TBasicProxy>
        class TProxy : public TBase {
        public:
            using TBase::TBase;

            template <class T>
            void Add(T x) {
                this->AddFunc([x](auto* m) {
                    m->Add(x);
                });
            }
            void Reset() {
                this->Reset();
            }
        };

        template <class T>
        void Add(T x) {
            for (auto& v : Values_) {
                if (x <= v.Threshold) {
                    ++v.Count;
                } else {
                    break;
                }
            }
        }

        void Visit(TFunc&& func) noexcept override {
            for (const auto& v : Values_) {
                func(GetFullName(v.Name), v.Count, true);
            }
        }
    private:
        virtual TString GetFullName(const TString& name) const = 0;

        TVector<TThreshold> Values_;
    };

    template <ui64... thresholds>
    class TLowerThresholdMetric : public TBasicLowerThresholdMetric<TLowerThresholdMetric<thresholds...>, thresholds...> {
    public:
        using TBasicLowerThresholdMetric<TLowerThresholdMetric<thresholds...>, thresholds...>::TBasicLowerThresholdMetric;
    private:
        TString GetFullName(const TString& name) const override {
            return TStringBuilder{} << this->Name() << "_" << name;
        }
    };

    template <ui64... thresholds>
    class TSolomonLowerThresholdMetric : public TBasicLowerThresholdMetric<TSolomonLowerThresholdMetric<thresholds...>, thresholds...> {
    public:
        using TBasicLowerThresholdMetric<TSolomonLowerThresholdMetric<thresholds...>, thresholds...>::TBasicLowerThresholdMetric;
    private:
        TString GetFullName(const TString& name) const override {
            return TStringBuilder{} << this->Name() << SOLOMON_LABEL_DELIMITER << "t" << SOLOMON_VALUE_DELIMITER << name;
        }
    };

    template <ui64... thresholds>
    class TSolomonThresholdMetric : public TBasicThresholdMetric<TSolomonThresholdMetric<thresholds...>, thresholds...> {
    public:
        using TBasicThresholdMetric<TSolomonThresholdMetric<thresholds...>, thresholds...>::TBasicThresholdMetric;
    private:
        TString GetFullName(const TString& name) const override {
            return TStringBuilder{} << this->Name() << SOLOMON_LABEL_DELIMITER << "t" << SOLOMON_VALUE_DELIMITER << name;
        }
    };

    template <class TDerived, ui64 duration, class T, int denom, int... nominators>
    class TBasicPercentileMetric : public TBasicMetric {
    public:
        using TBasicMetric::TBasicMetric;
        using TBasicProxy = TStats::TContext::TBasicProxy<TDerived>;

        template <class TBase = TBasicProxy>
        class TProxy : public TBase {
        public:
            using TBase::TBase;

            void Add(T v) {
                this->AddFunc([v, now = Now_](auto* m) {
                    m->Add(v, now);
                });
            }
            void Reset() {
                this->AddFunc([](auto* m) {
                    m->Reset();
                });
            }
        private:
            TInstant Now_ = TInstant::Now();
        };

        void Add(T v, TInstant now) noexcept {
            CleanUp(now);
            Deque_.emplace_back(now, v);
        }
        void Reset() noexcept {
            Deque_.clear();
        }
        void Visit(TFunc&& func) noexcept override {
            VisitRaw([&](double perc, double value) {
                func(GetMetricName(perc), value, false);
            });
        }
        void VisitRaw(std::function<void(double perc, double value)>&& fn) noexcept {
            CleanUp(TInstant::Now());
            if (Deque_.size() == 0) {
                return;
            }
            TVector<T> values;
            values.reserve(Deque_.size());
            for (const auto& d : Deque_) {
                values.push_back(d.second);
            }
            static const TVector<double> parts = {static_cast<double>(nominators) / denom ...};
            TVector<typename TVector<T>::iterator> poses;
            poses.reserve(parts.size());
            for (double p : parts) {
                poses.push_back(values.begin() + (ui64)((values.size() - 1) * p));
            }
            NthElements(values.begin(), values.end(), poses.begin(), poses.end());
            for (ui64 i = 0; i < parts.size(); ++i) {
                fn(parts[i], *poses[i]);
            }
        }
    private:
        virtual TString GetMetricName(double p) const = 0;
        void CleanUp(TInstant now) {
            while (!Deque_.empty() && Deque_.front().first + TDuration::Seconds(duration) < now) {
                Deque_.pop_front();
            }
        }
        TDeque<std::pair<TInstant, T>> Deque_;
    };

    template <ui64 duration, class T, int denom, int... nominators>
    class TPercentileMetric : public TBasicPercentileMetric<TPercentileMetric<duration, T, denom, nominators...>, duration, T, denom, nominators...> {
    public:
        using TSelf = TPercentileMetric<duration, T, denom, nominators...>;
        using TBasicPercentileMetric<TSelf, duration, T, denom, nominators...>::TBasicPercentileMetric;

    private:
        virtual TString GetMetricName(double p) const {
            return TStringBuilder{} << this->Name() << "_p_" << Prec(p * 100.0, PREC_NDIGITS, 5);
        }
    };

    template <ui64 duration, class T, int denom, int... nominators>
    class TSolomonPercentileMetric : public TBasicPercentileMetric<TSolomonPercentileMetric<duration, T, denom, nominators...>, duration, T, denom, nominators...> {
    public:
        using TSelf = TSolomonPercentileMetric<duration, T, denom, nominators...>;
        using TBasicPercentileMetric<TSelf, duration, T, denom, nominators...>::TBasicPercentileMetric;

    private:
        TString GetMetricName(double p) const override {
            return TStringBuilder{} << this->Name() << SOLOMON_LABEL_DELIMITER << "p" << SOLOMON_VALUE_DELIMITER << Prec(p * 100.0, PREC_NDIGITS, 5);
        }
    };

    // classes below is deprecated
    // use TStats + TSolomonContext + TSolomonOut

    class TQueueStats {
    public:
        struct TData {
            TInstant Time;
            double Value;
        };

        using TDequeType = TDeque<TData>;

        TQueueStats()
            : Duration_(TDuration::Minutes(1))
            , L_()
            , Deques_()
        {
        }

        explicit TQueueStats(TDuration duration)
            : Duration_(duration)
            , L_()
            , Deques_()
        {
        }

        void Add(TStringBuf name, double v);

        template <class TRange>
        void AddRange(const TRange& range) {
            with_lock (L_) {
                const auto& t = TInstant::Now();
                for (const auto& r : range) {
                    auto& d = Deques_[r.first];
                    CleanUp(&d);
                    d.push_back({t, static_cast<double>(r.second)});
                }
            }
        }

        template <class F>
        inline void Out(F&& f) {
            with_lock (L_) {
                for (auto& x : Deques_) {
                    CleanUp(&x.second);
                    const auto& y = x;
                    f(y.first, y.second);
                }
            }
        }

    private:
        void CleanUp(TDequeType* values, TInstant t = TInstant::Now()) {
            TInstant bound = t - Duration_;
            while (!values->empty() && values->front().Time < bound) {
                values->pop_front();
            }
        }

    private:
        TDuration Duration_;
        TAdaptiveLock L_;
        THashMap<TString, TDequeType> Deques_;
    };

    class TDictStats {
        TAdaptiveLock L_;
        THashMap<TString, THolder<TStats>> C_;

    public:
        TStats::TContext operator[](const TString& statKey) {
            with_lock (L_) {
                auto& stHolder = C_[statKey];
                if (!stHolder) {
                    stHolder.Reset(new TStats);
                }
                return TStats::TContext(*stHolder);
            }
        }

        /*
            F is
            void F(dictKey, valName, value)
         */
        template <class F>
        inline void Out(F&& userFunc) const {
            with_lock (L_) {
                for (const auto& x : C_) {
                    // has no support for deriv metrics
                    auto ff = [&x, &userFunc](const TString& name, ui64 value) {
                        userFunc(x.first, name, value);
                    };
                    x.second->Out(ff);
                }
            }
        }
    };
}
