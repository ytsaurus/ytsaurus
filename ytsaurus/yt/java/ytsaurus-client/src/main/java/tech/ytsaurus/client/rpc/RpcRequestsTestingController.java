package tech.ytsaurus.client.rpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpc.TRequestHeader;

public class RpcRequestsTestingController {
    private final Map<GUID, List<CapturedRequest>> requestIdToRequest = new HashMap<>();
    private final Map<String, List<GUID>> methodToRequestIds = new HashMap<>();
    private final List<GUID> startedTransactions = new ArrayList<>();

    public void addRequest(TRequestHeader requestHeader, Object requestBody) {
        synchronized (this) {
            GUID requestId = RpcUtil.fromProto(requestHeader.getRequestId());
            if (!requestIdToRequest.containsKey(requestId)) {
                requestIdToRequest.put(requestId, new ArrayList<>());
            }
            requestIdToRequest.get(requestId).add(new CapturedRequest(requestHeader, requestBody));

            String method = requestHeader.getMethod();
            if (!methodToRequestIds.containsKey(method)) {
                methodToRequestIds.put(method, new ArrayList<>());
            }
            methodToRequestIds.get(method).add(requestId);
        }
    }

    public List<CapturedRequest> getRequestsByMethod(String method) {
        synchronized (this) {
            if (!methodToRequestIds.containsKey(method)) {
                return Collections.emptyList();
            }

            List<GUID> requestIds = methodToRequestIds.get(method);
            List<CapturedRequest> result = new ArrayList<>();
            for (GUID requestId : requestIds) {
                result.addAll(requestIdToRequest.get(requestId));
            }

            return result;
        }
    }

    public void addStartedTransaction(GUID transactionId) {
        synchronized (this) {
            startedTransactions.add(transactionId);
        }
    }

    public List<GUID> getStartedTransactions() {
        synchronized (this) {
            return new ArrayList<>(startedTransactions);
        }
    }

    public void clear() {
        synchronized (this) {
            requestIdToRequest.clear();
            methodToRequestIds.clear();
        }
    }

    public static class CapturedRequest {
        private final TRequestHeader header;
        private final Object body;

        public CapturedRequest(TRequestHeader header, Object body) {
            this.header = header;
            this.body = body;
        }

        public TRequestHeader getHeader() {
            return header;
        }

        public Object getBody() {
            return body;
        }
    }
}
