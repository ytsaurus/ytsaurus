#pragma once

#include <yql/tools/yqlworker/interface/proto/task.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/yexception.h>

#include <memory>
#include <expected>

namespace NYql::NWorkerApi {

struct TTaskWorkerInfo {
    TString WorkerId;
    TString WorkerVersion;
    TString WorkerPid;
    TString WorkerHost;
};

class ITaskResultCallback {
public:
    virtual ~ITaskResultCallback() = default;
    virtual NThreading::TFuture<void> Notify(const NProto::TTaskResult& results, ui64 sentTime) = 0;
};

class ITaskHandle {
public:
    virtual ~ITaskHandle() = default;
    virtual TString GetTaskId() = 0;
    virtual TTaskWorkerInfo GetWorkerInfo() = 0;
    virtual std::expected<void, TString> Cancel() = 0;
};

class TRunTaskError {
public:
    enum class EReason: ui64 {
        ALREADY_RUNNING = 1,
        FAIL = 2,
        REJECTED = 3,
    };

    TRunTaskError(EReason reason, TString message)
        : Reason_(reason)
        , Message_(std::move(message))
    {
    }

    EReason GetReason() const {
        return Reason_;
    }

    const TString& GetMessage() const {
        return Message_;
    }

private:
    EReason Reason_;
    TString Message_;
};

class IWorkerApi {
public:
    virtual ~IWorkerApi() = default;

    virtual bool IsHealthy() const = 0;

    virtual std::expected<std::shared_ptr<ITaskHandle>, TRunTaskError> RunTask(NProto::ETaskAction taskAction, NProto::TTaskData&& task, std::weak_ptr<ITaskResultCallback> callback) = 0;
};


} // namespace NYql::NWorkerApi
