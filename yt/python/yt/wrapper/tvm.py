from yt.packages.requests.auth import AuthBase

from copy import deepcopy


class ServiceTicketAuth(AuthBase):
    PROXY_TVM_ID = 2031010

    def __init__(self, tvm_client):
        self._tvm_client = tvm_client
        self._proxy_tvm_id = ServiceTicketAuth.PROXY_TVM_ID

    def override_proxy_tvm_id(self, tvm_id):
        self._proxy_tvm_id = tvm_id

    def issue_service_ticket(self):
        return self._tvm_client.get_service_ticket_for(tvm_id=self._proxy_tvm_id)

    def _set_ticket(self, request):
        if self._tvm_client is not None:
            request.headers["X-Ya-Service-Ticket"] = self.issue_service_ticket()

    def handle_redirect(self, request, **kwargs):
        self._set_ticket(request)
        return request

    def __call__(self, request):
        self._set_ticket(request)
        request.register_hook("response", self.handle_redirect)
        return request

    def __deepcopy__(self, memo):
        result = type(self)(self._tvm_client)
        memo[id(self._tvm_client)] = self._tvm_client
        for k, v in self.__dict__.items():
            setattr(result, k, deepcopy(v, memo))
        return result
