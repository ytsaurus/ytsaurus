package ru.yandex.yt.ytclient.proxy.internal;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class HostPortTest {
    @Test
    public void testIpv4ParsesCorrectly() {
        String address = "0.0.0.0:2021";
        HostPort hostPort = HostPort.parse(address);
        assertThat(hostPort.getPort(), equalTo(2021));
        assertThat(hostPort.getHost(), equalTo("0.0.0.0"));
    }

    @Test
    public void testIpv6ParsesCorrectlyBracketed() {
        String address = "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443";
        HostPort hostPort = HostPort.parse(address);
        assertThat(hostPort.getPort(), equalTo(443));
        assertThat(hostPort.getHost(), equalTo("2001:db8:85a3:8d3:1319:8a2e:370:7348"));
    }

    @Test
    public void testIpv6ParsesCorrectlyUnbracketed() {
        String address = "2001:db8:85a3:8d3:1319:8a2e:370:7348";
        HostPort hostPort = HostPort.parse(address);
        assertThat(hostPort.getPort(), equalTo(9013)); // default port
        assertThat(hostPort.getHost(), equalTo("2001:db8:85a3:8d3:1319:8a2e:370:7348"));
    }

    @Test
    public void testIpv4toString() {
        String address = "0.0.0.0:2021";
        HostPort hostPort = HostPort.parse(address);
        assertThat(hostPort.toString(), equalTo(address));
    }

    @Test
    public void testIpv6BracketedToString() {
        String address = "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443";
        HostPort hostPort = HostPort.parse(address);
        assertThat(hostPort.toString(), equalTo(address));
    }

    @Test
    public void testIpv6UnbracketedToString() {
        String address = "2001:db8:85a3:8d3:1319:8a2e:370:7348";
        HostPort hostPort = HostPort.parse(address);
        assertThat(hostPort.toString(), equalTo(String.format("[%s]:9013", address))); // default port
    }

    @Test
    public void testEquals() {
        String address1 = "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443";
        String address2 = "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:444";
        HostPort hostPort1 = HostPort.parse(address1);
        HostPort hostPort2 = HostPort.parse(address1);
        HostPort hostPort3 = HostPort.parse(address2);
        assertThat(hostPort1, equalTo(hostPort2));
        assertThat(hostPort1, not(equalTo(hostPort3)));
    }
}
