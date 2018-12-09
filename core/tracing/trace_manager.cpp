#include "trace_manager.h"
#include "private.h"
#include "config.h"
#include "trace_service_proxy.h"

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler_thread.h>

#include <yt/core/net/address.h>
#include <yt/core/net/local_address.h>

#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

#include <yt/core/rpc/bus/channel.h>

#include <util/network/init.h>

#include <util/system/byteorder.h>

namespace NYT {
namespace NTracing {

using namespace NConcurrency;
using namespace NRpc;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TracingLogger;

////////////////////////////////////////////////////////////////////////////////

class TTraceManager::TImpl
{
public:
    TImpl()
        : InvokerQueue_(New<TInvokerQueue>(
            EventCount_,
            NProfiling::EmptyTagIds,
            true,
            false))
        , Thread_(New<TThread>(this))
        , Config_(New<TTraceManagerConfig>())
    { }

    void Start()
    {
        Thread_->Start();
        InvokerQueue_->SetThreadId(Thread_->GetId());
    }

    bool IsStarted() const
    {
        return Thread_->IsStarted();
    }

    void Configure(TTraceManagerConfigPtr config)
    {
        if (Y_UNLIKELY(!IsStarted())) {
            Start();
        }

        Config_ = std::move(config);

        if (Config_->Address) {
            Endpoint_ = GetLocalEndpoint();
            Channel_ = NRpc::NBus::CreateBusChannelFactory(Config_->BusClient)->CreateChannel(*Config_->Address);

            SendExecutor_ = New<TPeriodicExecutor>(
                InvokerQueue_,
                BIND(&TImpl::PushBatch, this),
                Config_->SendPeriod);
            SendExecutor_->Start();
        }
    }

    void Shutdown()
    {
        InvokerQueue_->Shutdown();
        Thread_->Shutdown();
    }

    void Enqueue(
        const NTracing::TTraceContext& context,
        const TString& serviceName,
        const TString& spanName,
        const TString& annotationName)
    {
        if (!IsEnqueueEnabled(context)) {
            return;
        }

        NProto::TTraceEvent event;
        event.set_trace_id(context.GetTraceId());
        event.set_span_id(context.GetSpanId());
        event.set_parent_span_id(context.GetParentSpanId());
        event.set_timestamp(ToProto<i64>(TInstant::Now()));
        event.set_service_name(serviceName);
        event.set_span_name(spanName);
        event.set_annotation_name(annotationName);
        EnqueueEvent(event);
    }

    void Enqueue(
        const NTracing::TTraceContext& context,
        const TString& annotationKey,
        const TString& annotationValue)
    {
        if (!IsEnqueueEnabled(context)) {
            return;
        }

        NProto::TTraceEvent event;
        event.set_trace_id(context.GetTraceId());
        event.set_span_id(context.GetSpanId());
        event.set_parent_span_id(context.GetParentSpanId());
        event.set_timestamp(ToProto<i64>(TInstant::Now()));
        event.set_annotation_key(annotationKey);
        event.set_annotation_value(annotationValue);
        EnqueueEvent(event);
    }

private:
    class TThread
        : public TSchedulerThread
    {
    public:
        explicit TThread(TImpl* owner)
            : TSchedulerThread(
                owner->EventCount_,
                "Tracing",
                NProfiling::EmptyTagIds,
                true,
                false)
            , Owner_(owner)
        { }

    private:
        TImpl* const Owner_;

        virtual EBeginExecuteResult BeginExecute() override
        {
            return Owner_->BeginExecute();
        }

        virtual void EndExecute() override
        {
            Owner_->EndExecute();
        }
    };

    const std::shared_ptr<TEventCount> EventCount_ = std::make_shared<TEventCount>();
    const TInvokerQueuePtr InvokerQueue_;
    const TIntrusivePtr<TThread> Thread_;
    TEnqueuedAction CurrentAction_;

    TMultipleProducerSingleConsumerLockFreeStack<NProto::TTraceEvent> EventQueue_;

    TTraceManagerConfigPtr Config_;
    NProto::TEndpoint Endpoint_;
    IChannelPtr Channel_;

    std::vector<NProto::TTraceEvent> CurrentBatch_;
    TPeriodicExecutorPtr SendExecutor_;


    EBeginExecuteResult BeginExecute()
    {
        auto result = InvokerQueue_->BeginExecute(&CurrentAction_);
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }

        int eventsProcessed = 0;
        while (EventQueue_.DequeueAll(true, [&] (NProto::TTraceEvent& event) {
                if (eventsProcessed == 0) {
                    EventCount_->CancelWait();
                }

                CurrentBatch_.push_back(std::move(event));
                ++eventsProcessed;

                if (CurrentBatch_.size() >= Config_->MaxBatchSize) {
                    PushBatch();
                }
            }))
        { }

        return eventsProcessed > 0
            ? EBeginExecuteResult::Success
            : EBeginExecuteResult::QueueEmpty;
    }

    void EndExecute()
    {
        InvokerQueue_->EndExecute(&CurrentAction_);
    }


    bool IsPushEnabled()
    {
        return Config_->Address.HasValue() && Channel_;
    }

    void PushBatch()
    {
        if (CurrentBatch_.empty()) {
            return;
        }

        if (!IsPushEnabled()) {
            CurrentBatch_.clear();
            return;
        }

        TTraceServiceProxy proxy(Channel_);
        auto req = proxy.SendBatch();
        req->SetTimeout(Config_->RpcTimeout);
        *req->mutable_endpoint() = Endpoint_;
        for (const auto& event : CurrentBatch_) {
            *req->add_events() = event;
        }

        LOG_DEBUG("Events sent proxy (Count: %v)",
            CurrentBatch_.size());

        req->Invoke();
        CurrentBatch_.clear();
    }

    bool IsEnqueueEnabled(const TTraceContext& context)
    {
        return context.IsVerbose() && Thread_->IsStarted() && !Thread_->IsShutdown();
    }

    void EnqueueEvent(const NProto::TTraceEvent& event)
    {
        if (Y_UNLIKELY(!IsStarted())) {
            Start();
        }

        if (event.has_annotation_key()) {
            LOG_DEBUG("Event %v=%v %08" PRIx64 ":%08" PRIx64 ":%08" PRIx64,
                event.annotation_key(),
                event.annotation_value(),
                event.trace_id(),
                event.span_id(),
                event.parent_span_id());
        } else {
            LOG_DEBUG("Event %v:%v %v %08" PRIx64 ":%08" PRIx64 ":%08" PRIx64,
                event.service_name(),
                event.span_name(),
                event.annotation_name(),
                event.trace_id(),
                event.span_id(),
                event.parent_span_id());
        }

        // XXX(babenko): queuing is temporarily disabled
#if 0
        EventQueue_.Enqueue(event);
        EventCount_->NotifyOne();
#endif
    }

    NProto::TEndpoint GetLocalEndpoint()
    {
        auto* addressResolver = TAddressResolver::Get();
        auto addressOrError = addressResolver->Resolve(GetLocalHostName()).Get();
        if (!addressOrError.IsOK()) {
            LOG_FATAL(addressOrError, "Error determining local endpoint address");
        }

        NProto::TEndpoint endpoint;
        const auto& sockAddr = addressOrError.Value().GetSockAddr();
        switch (sockAddr->sa_family) {
            case AF_INET: {
                auto* typedAddr = reinterpret_cast<const sockaddr_in*>(sockAddr);
                endpoint.set_address(LittleToBig<ui32>(typedAddr->sin_addr.s_addr));
                endpoint.set_port(Config_->EndpointPort);
                break;
            }
            case AF_INET6: {
                auto* typedAddr = reinterpret_cast<const sockaddr_in6*>(sockAddr);
                // hack: ipv6 -> ipv4 :)
                const ui32* fake = reinterpret_cast<const ui32*>(typedAddr->sin6_addr.s6_addr + 12);
                endpoint.set_address(LittleToBig(*fake));
                endpoint.set_port(Config_->EndpointPort);
                break;
            }

            default:
                LOG_FATAL("Neither v4 nor v6 address is known for local endpoint");
        }

        return endpoint;
    }
};

////////////////////////////////////////////////////////////////////////////////

TTraceManager::TTraceManager()
    : Impl_(new TImpl())
{ }

TTraceManager::~TTraceManager() = default;

TTraceManager* TTraceManager::Get()
{
    return Singleton<TTraceManager>();
}

void TTraceManager::StaticShutdown()
{
    Get()->Shutdown();
}

void TTraceManager::Configure(TTraceManagerConfigPtr config)
{
    Impl_->Configure(std::move(config));
}

void TTraceManager::Shutdown()
{
    Impl_->Shutdown();
}

void TTraceManager::Enqueue(
    const TTraceContext& context,
    const TString& serviceName,
    const TString& spanName,
    const TString& annotationName)
{
    Impl_->Enqueue(
        context,
        serviceName,
        spanName,
        annotationName);
}

void TTraceManager::Enqueue(
    const TTraceContext& context,
    const TString& annotationKey,
    const TString& annotationValue)
{
    Impl_->Enqueue(
        context,
        annotationKey,
        annotationValue);
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(8, TTraceManager::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

