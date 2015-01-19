#pragma once

#include "public.h"

#include <ytlib/scheduler/job_probe_service.pb.h>

#include <core/rpc/client.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TJobProbeServiceProxy
    : public NRpc::TProxyBase
{
public:
	static Stroka GetServiceName()
	{
		return "JobProbeService";
	}

	static int GetProtocolVersion()
	{
		return 0;
	}

	explicit TJobProbeServiceProxy(NRpc::IChannelPtr channel)
		: TProxyBase(channel, GetServiceName())
	{ }

	DEFINE_RPC_PROXY_METHOD(NProto, GenerateInputContext);
};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT