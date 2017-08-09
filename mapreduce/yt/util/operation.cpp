#include "operation.h"

#include <mapreduce/yt/client/operation_tracker.h>

#include <mapreduce/yt/common/fluent.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/serialize.h>

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TString GetOperationPath(const TOperationId& operationId) {
    auto opIdStr = GetGuidAsString(operationId);
    return Sprintf("//sys/operations/%s", ~opIdStr);
}

TString GetOperationStatusPath(const TOperationId& operation) {
    return GetOperationPath(operation) + "/@state";
}

EOperationStatus CheckOperation(
    NYT::IClientBasePtr client,
    const TOperationId& operationId)
{
    auto opIdStr = GetGuidAsString(operationId);
    auto opPath = GetOperationPath(operationId);
    Sprintf("//sys/operations/%s", ~opIdStr);
    auto statePath = GetOperationStatusPath(operationId);

    if (!client->Exists(opPath)) {
        ythrow yexception() << "Operation " << opIdStr << " does not exist";
    }

    TString state = client->Get(statePath).AsString();

    return CheckOperationStatus(client, operationId, state);
}

EOperationStatus CheckOperationStatus(
    NYT::IClientBasePtr client,
    const TOperationId& operationId,
    const TString& state)
{
    auto opIdStr = GetGuidAsString(operationId);
    auto opPath = GetOperationPath(operationId);

    if (state == "completed") {
        return OS_COMPLETED;

    } else if (state == "aborted" || state == "failed") {
        LOG_ERROR("Operation %s %s (%s)",
            ~opIdStr,
            ~state,
            ~ToString(TOperationExecutionTimeTracker::Get()->Finish(operationId)));

        auto errorPath = opPath + "/@result/error";
        TYtError error(TString("Unknown operation error"));
        if (client->Exists(errorPath)) {
            error = TYtError(client->Get(errorPath).AsString());
        }

        TStringStream jobErrors;
        DumpOperationStderrs(client, jobErrors, opPath);

        ythrow TOperationFailedError(
            state == "aborted" ?
                TOperationFailedError::Aborted :
                TOperationFailedError::Failed,
            operationId,
            error,
            {}) << jobErrors.Str();
    }

    return OS_RUNNING;
}

void DumpOperationStderrs(
    NYT::IClientBasePtr client,
    TOutputStream& stream,
    const TString& operationPath)
{
    const size_t RESULT_LIMIT = 1 << 20;
    const size_t BLOCK_SIZE = 16 << 10;
    const i64 STDERR_LIMIT = 64 << 10;
    const size_t STDERR_COUNT_LIMIT = 20;

    auto jobsPath = operationPath + "/jobs";
    if (!client->Exists(jobsPath)) {
        return;
    }

    auto jobList = client->List(jobsPath, TListOptions().AttributeFilter(
            TAttributeFilter()
                .AddAttribute("state")
                .AddAttribute("error")
                .AddAttribute("address"))
    );

    TBuffer buffer;
    TBufferOutput output(buffer);
    buffer.Reserve(RESULT_LIMIT);

    size_t count = 0;
    for (auto& job : jobList) {
        auto jobPath = jobsPath + "/" + job.AsString();
        auto& attributes = job.Attributes();
        output << Endl;

        if (!attributes.HasKey("state") || attributes["state"].AsString() != "failed") {
            continue;
        }
        if (attributes.HasKey("address")) {
            output << "Host: " << attributes["address"].AsString() << Endl;
        }
        if (attributes.HasKey("error")) {
            output << "Error: " << NodeToYsonString(attributes["error"]) << Endl;
        }

        auto stderrPath = jobPath + "/stderr";
        if (!client->Exists(stderrPath)) {
            continue;
        }

        output << "Stderr: " << Endl;
        if (buffer.Size() >= RESULT_LIMIT) {
            break;
        }

        TRichYPath path(stderrPath);
        i64 stderrSize = client->Get(stderrPath + "/@uncompressed_data_size").AsInt64();
        if (stderrSize > STDERR_LIMIT) {
            path.AddRange(
                TReadRange().LowerLimit(
                    TReadLimit().Offset(stderrSize - STDERR_LIMIT)));
        }
        IFileReaderPtr reader = client->CreateFileReader(path);

        auto pos = buffer.Size();
        auto left = RESULT_LIMIT - pos;
        while (left) {
            auto blockSize = Min(left, BLOCK_SIZE);
            buffer.Resize(pos + blockSize);
            auto bytes = reader->Load(buffer.Data() + pos, blockSize);
            left -= bytes;
            if (bytes != blockSize) {
                buffer.Resize(pos + bytes);
                break;
            }
            pos += bytes;
        }

        if (left == 0 || ++count == STDERR_COUNT_LIMIT) {
            break;
        }
    }

    stream.Write(buffer.Data(), buffer.Size());
    stream << Endl;
}

////////////////////////////////////////////////////////////////////////////////

}
