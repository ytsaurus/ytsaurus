#include "session_info.h"

#include <contrib/ydb/core/protos/replication.pb.h>

namespace NKikimr::NReplication::NController {

TSessionInfo::TSessionInfo()
    : Ready(false)
{
}

void TSessionInfo::SetReady() {
    Ready = true;
}

bool TSessionInfo::IsReady() const {
    return Ready;
}

void TSessionInfo::AttachWorker(const TWorkerId& id) {
    Workers.insert(id);
}

void TSessionInfo::DetachWorker(const TWorkerId& id) {
    Workers.erase(id);
}

const THashSet<TWorkerId>& TSessionInfo::GetWorkers() const {
    return Workers;
}

bool TSessionInfo::HasWorker(const TWorkerId& id) const {
    return Workers.contains(id);
}

TWorkerInfo::TWorkerInfo(NKikimrReplication::TRunWorkerCommand* cmd) {
    SetCommand(cmd);
}

void TWorkerInfo::SetCommand(NKikimrReplication::TRunWorkerCommand* cmd) {
    if (!cmd) {
        return;
    }

    if (!Command) {
        Command = MakeHolder<NKikimrReplication::TRunWorkerCommand>();
    }

    Command->Swap(cmd);
}

bool TWorkerInfo::HasCommand() const {
    return bool(Command);
}

const NKikimrReplication::TRunWorkerCommand* TWorkerInfo::GetCommand() const {
    return Command.Get();
}

void TWorkerInfo::AttachSession(ui32 nodeId) {
    Session = nodeId;
    DataEnded = false;
}

void TWorkerInfo::ClearSession() {
    Session.Clear();
}

bool TWorkerInfo::HasSession() const {
    return bool(Session);
}

ui32 TWorkerInfo::GetSession() const {
    Y_ABORT_UNLESS(Session.Defined());
    return *Session;
}

bool TWorkerInfo::IsDataEnded() const {
    return DataEnded;
}

void TWorkerInfo::SetDataEnded(bool value) {
    DataEnded = value;
}

void TWorkerInfo::SetHeartbeat(const TRowVersion& value) {
    Heartbeat = value;
}

bool TWorkerInfo::HasHeartbeat() const {
    return Heartbeat.Defined();
}

const TRowVersion& TWorkerInfo::GetHeartbeat() const {
    Y_ABORT_UNLESS(Heartbeat.Defined());
    return *Heartbeat;
}

}
