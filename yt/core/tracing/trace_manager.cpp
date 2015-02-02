#include "stdafx.h"
#include "config.h"
#include "trace_manager.h"
#include "trace_service_proxy.h"
#include "private.h"

#include <core/concurrency/scheduler_thread.h>
#include <core/concurrency/periodic_executor.h>

#include <core/misc/address.h>

#include <core/rpc/bus_channel.h>

#include <util/system/byteorder.h>

namespace NYT {
namespace NTracing {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TracingLogger;

////////////////////////////////////////////////////////////////////////////////

class TTraceManager::TImpl
{
public:
    TImpl()
        : InvokerQueue_(New<TInvokerQueue>(
            &EventCount_,
            NProfiling::EmptyTagIds,
            true,
            false))
        , Thread_(New<TThread>(this))
        , Config_(New<TTraceManagerConfig>())
    {
        Thread_->Start();
        InvokerQueue_->SetThreadId(Thread_->GetId());
    }

    void Configure(NYTree::INodePtr node, const NYTree::TYPath& path)
    {
        Config_ = New<TTraceManagerConfig>();
        Config_->Load(node, true, true, path);

        if (Config_->Address) {
            Endpoint_ = GetLocalEndpoint();
            Channel_ = NRpc::GetBusChannelFactory()->CreateChannel(*Config_->Address);

            SendExecutor_ = New<TPeriodicExecutor>(
                InvokerQueue_,
                BIND(&TImpl::PushBatch, this),
                Config_->SendPeriod);
            SendExecutor_->Start();
        }
    }

    void Configure(const Stroka& fileName, const NYPath::TYPath& path)
    {
        try {
            TIFStream stream(fileName);
            auto root = NYTree::ConvertToNode(&stream);
            auto node = NYTree::GetNodeByYPath(root, path);
            Configure(node, path);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error while configuring tracing");
        }
   }

    void Shutdown()
    {
        InvokerQueue_->Shutdown();
        Thread_->Shutdown();
    }

    void Enqueue(
        const NTracing::TTraceContext& context,
        const Stroka& serviceName,
        const Stroka& spanName,
        const Stroka& annotationName)
    {
        if (!IsEnqueueEnabled(context))
            return;

        NProto::TTraceEvent event;
        event.set_trace_id(context.GetTraceId());
        event.set_span_id(context.GetSpanId());
        event.set_parent_span_id(context.GetParentSpanId());
        event.set_timestamp(TInstant::Now().MicroSeconds());
        event.set_service_name(serviceName);
        event.set_span_name(spanName);
        event.set_annotation_name(annotationName);
        EnqueueEvent(event);
    }

    void Enqueue(
        const NTracing::TTraceContext& context,
        const Stroka& annotationKey,
        const Stroka& annotationValue)
    {
        if (!IsEnqueueEnabled(context))
            return;

        NProto::TTraceEvent event;
        event.set_trace_id(context.GetTraceId());
        event.set_span_id(context.GetSpanId());
        event.set_parent_span_id(context.GetParentSpanId());
        event.set_timestamp(TInstant::Now().MicroSeconds());
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
                &owner->EventCount_,
                "Tracing",
                NProfiling::EmptyTagIds,
                true,
                false)
            , Owner_(owner)
        { }

    private:
        TImpl* Owner_;

        virtual EBeginExecuteResult BeginExecute() override
        {
            return Owner_->BeginExecute();
        }

        virtual void EndExecute() override
        {
            Owner_->EndExecute();
        }
    };

    TEventCount EventCount_;
    TInvokerQueuePtr InvokerQueue_;
    TIntrusivePtr<TThread> Thread_;
    TEnqueuedAction CurrentAction_;

    TLockFreeQueue<NProto::TTraceEvent> EventQueue_;

    TTraceManagerConfigPtr Config_;
    NProto::TEndpoint Endpoint_;
    NRpc::IChannelPtr Channel_;

    std::vector<NProto::TTraceEvent> CurrentBatch_;
    TPeriodicExecutorPtr SendExecutor_;


    EBeginExecuteResult BeginExecute()
    {
        auto result = InvokerQueue_->BeginExecute(&CurrentAction_);
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }

        int eventsProcessed = 0;
        NProto::TTraceEvent event;
        while (EventQueue_.Dequeue(&event)) {
            CurrentBatch_.push_back(event);
            ++eventsProcessed;

            if (IsPushEnabled() && CurrentBatch_.size() >= Config_->MaxBatchSize) {
                PushBatch();
            }
        }

        if (eventsProcessed > 0) {
            EventCount_.CancelWait();
            return EBeginExecuteResult::Success;
        } else {
            return EBeginExecuteResult::QueueEmpty;
        }
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
        if (!IsPushEnabled()) {
            CurrentBatch_.clear();
            return;
        }

        if (CurrentBatch_.empty())
            return;

        TTraceServiceProxy proxy(Channel_);
        auto req = proxy.SendBatch();
        *req->mutable_endpoint() = Endpoint_;
        for (const auto& event : CurrentBatch_) {
            *req->add_events() = event;
        }

        LOG_DEBUG("Pushed %v events to proxy", CurrentBatch_.size());

        req->Invoke();
        CurrentBatch_.clear();
    }


    bool IsEnqueueEnabled(const TTraceContext& context)
    {
        return context.IsEnabled() && Thread_->IsRunning();
    }

    void EnqueueEvent(const NProto::TTraceEvent& event)
    {
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

        EventQueue_.Enqueue(event);
        EventCount_.NotifyOne();
    }


    NProto::TEndpoint GetLocalEndpoint()
    {
        auto* addressResolver = TAddressResolver::Get();
        auto addressOrError = addressResolver->Resolve(addressResolver->GetLocalHostName()).Get();
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

TTraceManager::~TTraceManager()
{ }

TTraceManager* TTraceManager::Get()
{
    return Singleton<TTraceManager>();
}

void TTraceManager::Configure(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    Impl_->Configure(node, path);
}

void TTraceManager::Configure(const Stroka& fileName, const NYPath::TYPath& path)
{
    Impl_->Configure(fileName, path);
}

void TTraceManager::Shutdown()
{
    Impl_->Shutdown();
}

void TTraceManager::Enqueue(
    const TTraceContext& context,
    const Stroka& serviceName,
    const Stroka& spanName,
    const Stroka& annotationName)
{
    Impl_->Enqueue(
        context,
        serviceName,
        spanName,
        annotationName);
}

void TTraceManager::Enqueue(
    const TTraceContext& context,
    const Stroka& annotationKey,
    const Stroka& annotationValue)
{
    Impl_->Enqueue(
        context,
        annotationKey,
        annotationValue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

