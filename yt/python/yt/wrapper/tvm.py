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


class UserTicketFixedAuth(AuthBase):

    def __init__(self, user_ticket=None):
        if user_ticket is not None:
            self.set_user_ticket(user_ticket)
        else:
            self._user_ticket = None

    def _validate_user_ticket(self, user_ticket):
        if user_ticket:
            parts = user_ticket.split(":", 2)
            if len(parts) == 3 and parts[1] == "user":
                return True
        return False

    def set_user_ticket(self, user_ticket):
        if self._validate_user_ticket(user_ticket):
            self._user_ticket = user_ticket
        else:
            raise ValueError()

    def _set_user_ticket(self, request):
        if self._user_ticket is not None:
            request.headers["X-Ya-User-Ticket"] = self._user_ticket

    def handle_redirect(self, request, **kwargs):
        self._set_user_ticket(request)
        return request

    def __call__(self, request):
        self._set_user_ticket(request)
        request.register_hook("response", self.handle_redirect)
        return request
