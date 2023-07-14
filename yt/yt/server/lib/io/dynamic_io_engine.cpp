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
                engineType,
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
        TSessionId sessionId,
        bool useDedicatedAllocations) override
    {
        return GetEngine()->Read(std::move(requests), category, tagCookie, sessionId, useDedicatedAllocations);
    }

    TFuture<void> Write(
        TWriteRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        return GetEngine()->Write(std::move(request), category, sessionId);
    }

    TFuture<void> FlushFile(
        TFlushFileRequest request,
        EWorkloadCategory category) override
    {
        return GetEngine()->FlushFile(std::move(request), category);
    }

    TFuture<void> FlushFileRange(
        TFlushFileRangeRequest request,
        EWorkloadCategory category,
        TSessionId sessionId) override
    {
        return GetEngine()->FlushFileRange(std::move(request), category, sessionId);
    }

    TFuture<void> FlushDirectory(
        TFlushDirectoryRequest request,
        EWorkloadCategory category) override
    {
        return GetEngine()->FlushDirectory(std::move(request), category);
    }

    TFuture<TIOEngineHandlePtr> Open(
        TOpenRequest request,
        EWorkloadCategory category) override
    {
        return GetEngine()->Open(std::move(request), category);
    }

    TFuture<void> Close(
        TCloseRequest request,
        EWorkloadCategory category) override
    {
        return GetEngine()->Close(std::move(request), category);
    }

    TFuture<void> Allocate(
        TAllocateRequest request,
        EWorkloadCategory category) override
    {
        return GetEngine()->Allocate(std::move(request), category);
    }

    TFuture<void> Lock(
        TLockRequest request,
        EWorkloadCategory category) override
    {
        return GetEngine()->Lock(std::move(request), category);
    }

    TFuture<void> Resize(
        TResizeRequest request,
        EWorkloadCategory category) override
    {
        return GetEngine()->Resize(std::move(request), category);
    }

    bool IsSick() const override
    {
        return GetEngine()->IsSick();
    }

    void SetType(EIOEngineType type) override
    {
        State_->CurrentType = type;
    }

    void Reconfigure(const NYTree::INodePtr& dynamicIOConfig) override
    {
        ForAllEngines([dynamicIOConfig] (const IIOEnginePtr& engine) {
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
        ForAllEngines([&] (const IIOEnginePtr& engine) {
            total += engine->GetTotalReadBytes();
        });
        return total;
    }

    i64 GetTotalWrittenBytes() const override
    {
        i64 total = 0;
        ForAllEngines([&] (const IIOEnginePtr& engine) {
            total += engine->GetTotalWrittenBytes();
        });
        return total;
    }

    EDirectIOPolicy UseDirectIOForReads() const override
    {
        return GetEngine()->UseDirectIOForReads();
    }

private:
    struct TEngineState final
    {
        explicit TEngineState(EIOEngineType type)
            : CurrentType(type)
        { }

        std::atomic<EIOEngineType> CurrentType;
        TEnumIndexedVector<EIOEngineType, IIOEnginePtr> Engines;
    };

    struct TProxyInvoker
        : public IInvoker
    {
        explicit TProxyInvoker(TIntrusivePtr<TEngineState> state)
            : State(state)
        { }

        TIntrusivePtr<TEngineState> State;

        void Invoke(TClosure callback) override
        {
            return State->Engines[State->CurrentType.load()]->GetAuxPoolInvoker()->Invoke(callback);
        }

        void Invoke(TMutableRange<TClosure> callbacks) override
        {
            for (auto& callback : callbacks) {
                Invoke(std::move(callback));
            }
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

        bool IsSerialized() const override
        {
            for (auto engine : State->Engines) {
                if (engine && !engine->GetAuxPoolInvoker()->IsSerialized()) {
                    return false;
                }
            }

            return true;
        }

        void RegisterWaitTimeObserver(TWaitTimeObserver /*waitTimeObserver*/) override
        { }
    };

    TIntrusivePtr<TEngineState> State_;
    TIntrusivePtr<TProxyInvoker> ProxyInvoker_;
    IInvokerPtr AuxPoolInvoker_;

    const IIOEnginePtr& GetEngine() const
    {
        return State_->Engines[State_->CurrentType.load()];
    }

    template <class TFn>
    void ForAllEngines(const TFn& cb) const
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
