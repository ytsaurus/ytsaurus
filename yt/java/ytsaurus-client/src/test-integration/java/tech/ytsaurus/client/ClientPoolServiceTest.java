package tech.ytsaurus.client;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

public class ClientPoolServiceTest {

    @Test
    public void testHttpBuilder() {
        ClientPoolService.HttpBuilder builder = ClientPoolService.httpBuilder();
        builder.setBalancerFqdn("host").setBalancerPort(123);
        assertThat(builder.balancerFqdn, is("host"));
        assertThat(builder.balancerPort, is(123));

        builder.setBalancerFqdn("2a02:6b8:c27:12d8:0:f411:0:1f").setBalancerPort(123);
        assertThat(builder.balancerFqdn, is("[2a02:6b8:c27:12d8:0:f411:0:1f]"));
        assertThat(builder.balancerPort, is(123));

        builder.setBalancerFqdn("[2a02:6b8:c27:12d8:0:f411:0:1f]").setBalancerPort(123);
        assertThat(builder.balancerFqdn, is("[2a02:6b8:c27:12d8:0:f411:0:1f]"));
        assertThat(builder.balancerPort, is(123));

        builder.setBalancerFqdn("[2a02:6b8:c10:1605:0:f408::]").setBalancerPort(27004);
        assertThat(builder.balancerFqdn, is("[2a02:6b8:c10:1605:0:f408::]"));
        assertThat(builder.balancerPort, is(27004));

        builder.setBalancerFqdn("[::1]").setBalancerPort(8080);
        assertThat(builder.balancerFqdn, is("[::1]"));
        assertThat(builder.balancerPort, is(8080));

        assertThrows(IllegalArgumentException.class,
                () -> builder.setBalancerFqdn("[2a02:6b8:c27:12d8:0:f411:0:1f]:123"));

        assertThrows(IllegalArgumentException.class,
                () -> builder.setBalancerFqdn("[192.168.0.1:80]"));

        assertThrows(IllegalArgumentException.class,
                () -> builder.setBalancerPort(10000000));
    }
}
