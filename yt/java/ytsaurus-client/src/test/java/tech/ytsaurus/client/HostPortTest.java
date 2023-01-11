package tech.ytsaurus.client;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class HostPortTest {
    @Test
    public void testIpv4ParsesCorrectly() {
        String address = "0.0.0.0:2021";
        HostPort hostPort = HostPort.parse(address);
        assertEquals(hostPort.getPort(), 2021);
        assertEquals(hostPort.getHost(), "0.0.0.0");
    }

    @Test
    public void testIpv6ParsesCorrectlyBracketed() {
        String address = "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443";
        HostPort hostPort = HostPort.parse(address);
        assertEquals(hostPort.getPort(), 443);
        assertEquals(hostPort.getHost(), "2001:db8:85a3:8d3:1319:8a2e:370:7348");
    }

    @Test
    public void testIpv6ParsesCorrectlyUnbracketed() {
        String address = "2001:db8:85a3:8d3:1319:8a2e:370:7348";
        HostPort hostPort = HostPort.parse(address);
        assertEquals(hostPort.getPort(), 9013); // default port
        assertEquals(hostPort.getHost(), "2001:db8:85a3:8d3:1319:8a2e:370:7348");
    }

    @Test
    public void testIpv4toString() {
        String address = "0.0.0.0:2021";
        HostPort hostPort = HostPort.parse(address);
        assertEquals(hostPort.toString(), address);
    }

    @Test
    public void testIpv6BracketedToString() {
        String address = "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443";
        HostPort hostPort = HostPort.parse(address);
        assertEquals(hostPort.toString(), address);
    }

    @Test
    public void testIpv6UnbracketedToString() {
        String address = "2001:db8:85a3:8d3:1319:8a2e:370:7348";
        HostPort hostPort = HostPort.parse(address);
        assertEquals(hostPort.toString(), String.format("[%s]:9013", address)); // default port
    }

    @Test
    public void testEquals() {
        String address1 = "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:443";
        String address2 = "[2001:db8:85a3:8d3:1319:8a2e:370:7348]:444";
        HostPort hostPort1 = HostPort.parse(address1);
        HostPort hostPort2 = HostPort.parse(address1);
        HostPort hostPort3 = HostPort.parse(address2);
        assertEquals(hostPort1, hostPort2);
        assertNotEquals(hostPort1, hostPort3);
    }
}
