#pragma once

#include <contrib/ydb/library/yql/providers/dq/api/protos/service.pb.h>
#include <yql/tools/yqlworker/config/config.h>
#include <yql/tools/yqlworker/misc/duplex_ipc_channel.h>

#include <util/generic/ptr.h>

#include <library/cpp/messagebus/network.h>

namespace NYql {

struct TMainConfig;

using TBindResultVector = TVector<NBus::TBindResult>;

class TWorkerDq {
public:

    TWorkerDq(
        const TMainConfig& config,
        const TBindResultVector& serverPorts);
    ~TWorkerDq();

    int Run();

private:
    class TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NYql
