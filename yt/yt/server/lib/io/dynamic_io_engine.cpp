#include "dynamic_io_engine.h"

#include "io_engine.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

class TDynamicIOEngine
    : public IDynamicIOEngine
{
public:
    TDynamicIOEngine(
        EIOEngineType defaultEngineType,
        NYTree::INodePtr ioConfig,
        TString locationId,
        NProfiling::TProfiler profiler,
        NLogging::TLogger logger)
        : State_(New<TEngineState>(defaultEngineType))
        , ProxyInvoker_(New<TProxyInvoker>(State_))
        , AuxPoolInvoker_(ProxyInvoker_)
    {
        for (auto engineType : GetSupportedIOEngineTypes()) {
            State_->Engines[engineType] = CreateIOEngine(
                defaultEngineType,
                ioConfig,
                locationId,
                profiler,
                logger);

            profiler
                .WithRequiredTag("engine_type", FormatEnum(engineType))
                .AddFuncGauge("/engine_enabled", MakeStrong(this), [this, engineType] {
                    return State_->CurrentType == engineType;
                });
        }
    }

    TFuture<TReadResponse> Read(
        std::vector<TReadRequest> requests,
        EWorkloadCategory category,
        TRefCountedTypeCookie tagCookie,
        TSessionId sessionId) override
    {
        return Engine()->Read(requests, category, tagCookie, sessionId);
    }

    TFuture<void> Write(
        TWriteRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle,
        TSessionId sessionId = {}) override
    {
        return Engine()->Write(request, category, sessionId);
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) override
    {
        return Engine()->FlushFile(request, category);
    }

    TFuture<void> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle,
        TSessionId sessionId = {}) override
    {
        return Engine()->FlushFileRange(request, category, sessionId);
    }

    TFuture<void> FlushDirectory(
        TFlushDirectoryRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) override
    {
        return Engine()->FlushDirectory(request, category);
    }

    TFuture<TIOEngineHandlePtr> Open(
        TOpenRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) override
    {
        return Engine()->Open(request, category);
    }

    TFuture<void> Close(
        TCloseRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) override
    {
        return Engine()->Close(request, category);
    }

    TFuture<void> Allocate(
        TAllocateRequest request,
        EWorkloadCategory category = EWorkloadCategory::Idle) override
    {
        return Engine()->Allocate(request, category);
    }

    bool IsSick() const override
    {
        return Engine()->IsSick();
    }

    void ReconfigureType(EIOEngineType type) override
    {
        State_->CurrentType = type;
    }

    void Reconfigure(const NYTree::INodePtr& dynamicIOConfig) override
    {
        ForAll([dynamicIOConfig] (const IIOEnginePtr& engine) {
            return engine->Reconfigure(dynamicIOConfig);
        });
    }

    const IInvokerPtr& GetAuxPoolInvoker() override
    {
        return AuxPoolInvoker_;
    }

    i64 GetTotalReadBytes() const override
    {
        i64 total = 0;
        ForAll([&] (const IIOEnginePtr& engine) {
            total += engine->GetTotalReadBytes();
        });
        return total;
    }

    i64 GetTotalWrittenBytes() const override
    {
        i64 total = 0;
        ForAll([&] (const IIOEnginePtr& engine) {
            total += engine->GetTotalWrittenBytes();
        });
        return total;
    }

private:
    struct TEngineState final
    {
        TEngineState(EIOEngineType type)
            : CurrentType(type)
        { }

        std::atomic<EIOEngineType> CurrentType;
        TEnumIndexedVector<EIOEngineType, IIOEnginePtr> Engines;
    };

    struct TProxyInvoker
        : public IInvoker
    {
        TProxyInvoker(TIntrusivePtr<TEngineState> state)
            : State(state)
        { }

        TIntrusivePtr<TEngineState> State;

        void Invoke(TClosure callback) override
        {
            return State->Engines[State->CurrentType.load()]->GetAuxPoolInvoker()->Invoke(callback);
        }

        NConcurrency::TThreadId GetThreadId() const override
        {
            return NConcurrency::InvalidThreadId;
        }

        bool CheckAffinity(const IInvokerPtr& invoker) const override
        {
            if (this == invoker) {
                return true;
            }

            for (auto engine : State->Engines) {
                if (!engine) {
                    continue;
                }

                if (engine->GetAuxPoolInvoker()->CheckAffinity(invoker)) {
                    return true;
                }
            }

            return false;
        }
    };

    TIntrusivePtr<TEngineState> State_;
    TIntrusivePtr<TProxyInvoker> ProxyInvoker_;
    IInvokerPtr AuxPoolInvoker_;

    const IIOEnginePtr& Engine() const
    {
        return State_->Engines[State_->CurrentType.load()];
    }

    template <class TFn>
    void ForAll(const TFn& cb) const
    {
        for (const auto& engine : State_->Engines) {
            if (engine) {
                cb(engine);
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicIOEngine)

////////////////////////////////////////////////////////////////////////////////

IDynamicIOEnginePtr CreateDynamicIOEngine(
    EIOEngineType defaultEngineType,
    NYTree::INodePtr ioConfig,
    TString locationId,
    NProfiling::TProfiler profiler,
    NLogging::TLogger logger)
{
    return New<TDynamicIOEngine>(
        defaultEngineType,
        ioConfig,
        locationId,
        profiler,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
