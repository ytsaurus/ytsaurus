package ru.yandex.yt.ytclient.rpc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.yt.rpc.TRequestHeader;

public class RpcRequestsTestingController {
    private Map<GUID, List<Pair<TRequestHeader, Object>>> requestIdToRequest = new HashMap<>();
    private Map<String, List<GUID>> methodToRequestIds = new HashMap<>();

    public void addRequest(TRequestHeader requestHeader, Object requestBody) {
        synchronized (this) {
            GUID requestId = RpcUtil.fromProto(requestHeader.getRequestId());
            if (!requestIdToRequest.containsKey(requestId)) {
                requestIdToRequest.put(requestId, new ArrayList<>());
            }
            requestIdToRequest.get(requestId).add(new ImmutablePair<>(requestHeader, requestBody));

            String method = requestHeader.getMethod();
            if (!methodToRequestIds.containsKey(method)) {
                methodToRequestIds.put(method, new ArrayList<>());
            }
            methodToRequestIds.get(method).add(requestId);
        }
    }

    public List<Pair<TRequestHeader, Object>> getRequestsByMethod(String method) {
        synchronized (this) {
            if (!methodToRequestIds.containsKey(method)) {
                return Arrays.asList();
            }

            List<GUID> requestIds = methodToRequestIds.get(method);
            List<Pair<TRequestHeader, Object>> result = new ArrayList<>();
            for (GUID requestId : requestIds) {
                result.addAll(requestIdToRequest.get(requestId));
            }

            return result;
        }
    }


    public void clear() {
        synchronized (this) {
            requestIdToRequest.clear();
            methodToRequestIds.clear();
        }
    }
}
