package tech.ytsaurus.client.rpc;

import ru.yandex.passport.tvmauth.TvmClient;

public class ServiceTicketClientAuth implements ServiceTicketAuth {
    private static final int PROXY_TVM_ID = 2031010;
    private final TvmClient tvmClient;

    public ServiceTicketClientAuth(TvmClient tvmClient) {
        this.tvmClient = tvmClient;
    }

    @Override
    public String issueServiceTicket() {
        return tvmClient.getServiceTicketFor(PROXY_TVM_ID);
    }
}
