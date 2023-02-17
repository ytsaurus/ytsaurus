package tech.ytsaurus.client.rpc;

public class ServiceTicketFixedAuth implements ServiceTicketAuth {
    private final String ticket;

    public ServiceTicketFixedAuth(String ticket) {
        this.ticket = ticket;
    }

    @Override
    public String issueServiceTicket() {
        return ticket;
    }
}
