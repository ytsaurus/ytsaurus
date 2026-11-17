#include "allocate_workers_response.h"

#include <yt/yql/providers/dq/common/attrs/attrs.h>

namespace NYql::NDqs {
    THolder<TEvAllocateWorkersResponse> MakeAllocateWorkersResponse(ui64 resourceId, const TVector<TWorkerInfo::TPtr>& infos) {
        auto response = MakeHolder<TEvAllocateWorkersResponse>(resourceId, TVector<ui32>{});
        auto& group = *response->Record.MutableNodes();
        for (const auto& workerInfo : infos) {
            auto* worker = group.AddWorker();
            *worker->MutableGuid() = GetGuidAsString(workerInfo->WorkerId); // for debug only
            worker->SetNodeId(workerInfo->NodeId);
            worker->SetClusterName(workerInfo->ClusterName);

            for (const auto& [k, v] : workerInfo->Attributes) {
                auto* attr = worker->AddAttribute();
                attr->SetKey(k);
                attr->SetValue(v);

                if (k == NCommonAttrs::JOBID_ATTR) {
                    worker->SetJobId(v);
                }
                if (k == NCommonAttrs::OPERATIONID_ATTR) {
                    worker->SetOperationId(v);
                }
            }
        }
        return response;
    }

}
