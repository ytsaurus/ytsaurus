package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.rpc.RpcCredentials;
import tech.ytsaurus.client.rpc.ServiceTicketClientAuth;
import tech.ytsaurus.core.cypress.CypressNodeType;

import ru.yandex.passport.tvmauth.BlackboxEnv;
import ru.yandex.passport.tvmauth.CheckedServiceTicket;
import ru.yandex.passport.tvmauth.CheckedUserTicket;
import ru.yandex.passport.tvmauth.ClientStatus;
import ru.yandex.passport.tvmauth.TvmClient;
import ru.yandex.passport.tvmauth.roles.Roles;
import ru.yandex.yt.testlib.LocalYt;

public class ServiceTicketAuthTest {
    private static final String SERVICE_TICKET =
            "3:serv:CBAQ__________9_IgYIlJEGECo:O9-vbod_8czkKrpwJAZCI8UgOIhNr2xKPcS" +
                    "-LWALrVC224jga2nIT6vLiw6q3d6pAT60g9K7NB39LEmh7vMuePtUMjzuZuL" +
                    "-uJg17BsH2iTLCZSxDjWxbU9piA2T6u607jiSyiy-FI74pEPqkz7KKJ28aPsefuC1VUweGkYFzNY";

    private static class MockedTvmClient implements TvmClient {

        @Override
        public ClientStatus getStatus() {
            return null;
        }

        @Override
        public String getServiceTicketFor(String alias) {
            return null;
        }

        @Override
        public String getServiceTicketFor(int tvmId) {
            return SERVICE_TICKET;
        }

        @Override
        public CheckedServiceTicket checkServiceTicket(String ticketBody) {
            return null;
        }

        @Override
        public CheckedUserTicket checkUserTicket(String ticketBody) {
            return null;
        }

        @Override
        public CheckedUserTicket checkUserTicket(String ticketBody, BlackboxEnv overridedBbEnv) {
            return null;
        }

        @Override
        public Roles getRoles() {
            return null;
        }

        @Override
        public void close() {
        }
    }

    private static class ServiceTicketAuthTestWrapper extends ServiceTicketClientAuth {
        private final List<String> issuedServiceTickets = new ArrayList<>();

        ServiceTicketAuthTestWrapper(TvmClient tvmClient) {
            super(tvmClient);
        }

        @Override
        public String issueServiceTicket() {
            String ticket = super.issueServiceTicket();
            issuedServiceTickets.add(ticket);
            return ticket;
        }

        public List<String> getIssuedServiceTickets() {
            return issuedServiceTickets;
        }
    }

    @Test
    public void testAuthenticationWithTvmClient() {
        ServiceTicketAuthTestWrapper serviceTicketAuth =
                new ServiceTicketAuthTestWrapper(new MockedTvmClient());
        var client = YtClient.builder()
                .setCluster(LocalYt.getAddress())
                .setRpcCredentials(new RpcCredentials(serviceTicketAuth))
                .build();

        try (client) {
            client.createNode("//tmp/test_node", CypressNodeType.MAP).join();
        }

        List<String> issuedServiceTickets = serviceTicketAuth.getIssuedServiceTickets();
        Assert.assertFalse(issuedServiceTickets.isEmpty());
        Assert.assertEquals(issuedServiceTickets.get(0), SERVICE_TICKET);
    }
}
