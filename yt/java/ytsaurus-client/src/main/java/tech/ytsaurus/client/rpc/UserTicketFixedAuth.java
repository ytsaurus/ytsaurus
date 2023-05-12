package tech.ytsaurus.client.rpc;

public class UserTicketFixedAuth implements UserTicketAuth {
    private final String ticket;

    public UserTicketFixedAuth(String ticket) {
        this.ticket = ticket;
    }

    @Override
    public String issueUserTicket() {
        return ticket;
    }
}
