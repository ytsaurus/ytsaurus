#include "stdafx.h"
#include "service.h"
#include "rpc.pb.h"

#include <ytlib/logging/log.h>
#include <ytlib/ytree/ypath_client.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NProto;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("RPC");
static NProfiling::TProfiler Profiler("rpc/server");

////////////////////////////////////////////////////////////////////////////////

void IServiceContext::Reply(NBus::IMessage* message)
{
    auto parts = message->GetParts();
    YASSERT(!parts.empty());

    TResponseHeader header;
    if (!DeserializeProtobuf(&header, parts[0])) {
        LOG_FATAL("Error deserializing response header");
    }

    TError error(
        header.error_code(),
        header.has_error_message() ? header.error_message() : "");

    if (error.IsOK()) {
        YASSERT(parts.ysize() >= 2);

        SetResponseBody(parts[1]);

        parts.erase(parts.begin(), parts.begin() + 2);
        ResponseAttachments() = MoveRV(parts);
    }

    Reply(error);
}

////////////////////////////////////////////////////////////////////////////////

class TServiceBase::TImpl
{
public:
	TImpl(
		IInvoker* defaultInvoker,
		const Stroka& serviceName,
		const Stroka& loggingCategory)
		: DefaultInvoker(defaultInvoker)
		, ServiceName(serviceName)
		, ServiceLogger(loggingCategory)
	{
		YASSERT(defaultInvoker);
	}

	virtual Stroka GetServiceName() const
	{
		return ServiceName;
	}

	virtual Stroka GetLoggingCategory() const
	{
		return ServiceLogger.GetCategory();
	}


	virtual void OnBeginRequest(IServiceContext* context)
	{
		YASSERT(context);

		Stroka verb = context->GetVerb();

		TGuard<TSpinLock> guard(SpinLock);

		auto methodIt = RuntimeMethodInfos.find(verb);
		if (methodIt == RuntimeMethodInfos.end()) {
			guard.Release();

			Stroka message = Sprintf("Unknown verb (ServiceName: %s, Verb: %s)",
				~ServiceName,
				~verb);
			LOG_WARNING("%s", ~message);
			if (!context->IsOneWay()) {
				context->Reply(TError(EErrorCode::NoSuchVerb, message));
			}

			return;
		}

		auto runtimeInfo = methodIt->second;
		if (runtimeInfo->Descriptor.OneWay != context->IsOneWay()) {
			guard.Release();

			Stroka message = Sprintf("One-way flag mismatch (Expected: %s, Actual: %s, ServiceName: %s, Verb: %s)",
				~ToString(runtimeInfo->Descriptor.OneWay),
				~ToString(context->IsOneWay()),
				~ServiceName,
				~verb);
			LOG_WARNING("%s", ~message);
			if (!context->IsOneWay()) {
				context->Reply(TError(EErrorCode::NoSuchVerb, message));
			}

			return;
		}

		auto timer = Profiler.TimingStart(CombineYPaths(
			context->GetPath(),
			context->GetVerb(),
			"time"));

		auto activeRequest = New<TActiveRequest>(runtimeInfo, timer);

		if (!context->IsOneWay()) {
			YVERIFY(ActiveRequests.insert(MakePair(context, activeRequest)).second);
		}

		guard.Release();

		auto handler = runtimeInfo->Descriptor.Handler;
		auto wrappedHandler = context->Wrap(~handler->Bind(context));

		runtimeInfo->Invoker->Invoke(FromFunctor([=] ()
		    {
			    auto& timer = activeRequest->Timer;
			    Profiler.TimingCheckpoint(timer, "wait");

                // No need for a lock here.
                activeRequest->Running = true;

			    wrappedHandler->Do();
			    Profiler.TimingCheckpoint(timer, "sync");

                {
                    TGuard<TSpinLock> guard(activeRequest->SpinLock);
                    YASSERT(activeRequest->Running);
                    activeRequest->Running = false;
                    if (activeRequest->Completed || runtimeInfo->Descriptor.OneWay) {
                        Profiler.TimingStop(timer);
                    }
                }
		    }));
	}

	virtual void OnEndRequest(IServiceContext* context)
	{
		YASSERT(context);
		YASSERT(!context->IsOneWay());

		TGuard<TSpinLock> guard(SpinLock);

		auto it = ActiveRequests.find(context);
		if (it == ActiveRequests.end())
			return;

		auto& activeRequest = it->second;

		auto& timer = activeRequest->Timer;
		Profiler.TimingCheckpoint(timer, "async");

        {
            TGuard<TSpinLock> guard(activeRequest->SpinLock);
            YASSERT(!activeRequest->Completed);
            activeRequest->Completed = true;
            if (!activeRequest->Running) {
                Profiler.TimingStop(timer);
            }
        }

		ActiveRequests.erase(it);
	}

    void RegisterMethod(const TMethodDescriptor& descriptor)
	{
		RegisterMethod(descriptor, ~DefaultInvoker);
	}

    void RegisterMethod(const TMethodDescriptor& descriptor, IInvoker* invoker)
	{
		YASSERT(invoker);

		TGuard<TSpinLock> guard(SpinLock);
		auto info = New<TRuntimeMethodInfo>(descriptor,invoker);
		// Failure here means that such verb is already registered.
		YVERIFY(RuntimeMethodInfos.insert(MakePair(descriptor.Verb, info)).second);
	}

private:
	struct TRuntimeMethodInfo
		: public TIntrinsicRefCounted
	{
		TRuntimeMethodInfo(const TMethodDescriptor& info, IInvoker* invoker)
			: Descriptor(info)
			, Invoker(invoker)
		{ }

		TMethodDescriptor Descriptor;
		IInvoker::TPtr Invoker;
	};

	typedef TIntrusivePtr<TRuntimeMethodInfo> TRuntimeMethodInfoPtr;

	struct TActiveRequest
		: public TIntrinsicRefCounted
	{
		TActiveRequest(
			TRuntimeMethodInfoPtr runtimeInfo,
			const NProfiling::TTimer& timer)
			: RuntimeInfo(runtimeInfo)
			, Timer(timer)
            , Running(false)
            , Completed(false)
		{ }

		TRuntimeMethodInfoPtr RuntimeInfo;
		NProfiling::TTimer Timer;

        //! Guards #Running and #Replied.
        TSpinLock SpinLock;
        //! True if the service method is currently running.
        bool Running;
        //! True if #OnEndRequest is already called.
        bool Completed;
	};

	typedef TIntrusivePtr<TActiveRequest> TActiveRequestPtr;

    IInvoker::TPtr DefaultInvoker;
    Stroka ServiceName;
    NLog::TLogger ServiceLogger;

    //! Protects #RuntimeMethodInfos and #ActiveRequests.
    TSpinLock SpinLock;
    yhash_map<Stroka, TRuntimeMethodInfoPtr> RuntimeMethodInfos;
    yhash_map<IServiceContext::TPtr, TActiveRequestPtr> ActiveRequests;

};

TServiceBase::TServiceBase(
    IInvoker* defaultInvoker,
    const Stroka& serviceName,
    const Stroka& loggingCategory)
	: Impl(new TImpl(
		defaultInvoker,
		serviceName,
		loggingCategory))
{ }

TServiceBase::~TServiceBase()
{ }

void TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor, IInvoker* invoker)
{
	Impl->RegisterMethod(descriptor, invoker);
}

void TServiceBase::RegisterMethod(const TMethodDescriptor& descriptor)
{
	Impl->RegisterMethod(descriptor);
}

void TServiceBase::OnBeginRequest(IServiceContext* context)
{
	Impl->OnBeginRequest(context);
}

void TServiceBase::OnEndRequest(IServiceContext* context)
{
	Impl->OnEndRequest(context);
}

Stroka TServiceBase::GetServiceName() const
{
	return Impl->GetServiceName();
}

Stroka TServiceBase::GetLoggingCategory() const
{
	return Impl->GetLoggingCategory();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
