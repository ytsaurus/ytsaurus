package tech.ytsaurus.client;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

public class ClientPoolServiceTest {

    @Test
    public void testHttpBuilder() {
        ClientPoolService.HttpBuilder builder = ClientPoolService.httpBuilder();
        builder.setBalancerAddress("host", 123);
        assertThat(builder.balancerAddress, is("host:123"));

        builder.setBalancerAddress("2a02:6b8:c27:12d8:0:f411:0:1f", 123);
        assertThat(builder.balancerAddress, is("[2a02:6b8:c27:12d8:0:f411:0:1f]:123"));

        builder.setBalancerAddress("[2a02:6b8:c27:12d8:0:f411:0:1f]", 123);
        assertThat(builder.balancerAddress, is("[2a02:6b8:c27:12d8:0:f411:0:1f]:123"));

        builder.setBalancerAddress("[2a02:6b8:c10:1605:0:f408::]", 27004);
        assertThat(builder.balancerAddress, is("[2a02:6b8:c10:1605:0:f408::]:27004"));

        builder.setBalancerAddress("[::1]", 8080);
        assertThat(builder.balancerAddress, is("[::1]:8080"));

        assertThrows(IllegalArgumentException.class,
                () -> builder.setBalancerAddress("[2a02:6b8:c27:12d8:0:f411:0:1f]:123", 123));

        assertThrows(IllegalArgumentException.class,
                () -> builder.setBalancerAddress("[192.168.0.1:80]", 80));

        assertThrows(IllegalArgumentException.class,
                () -> builder.setBalancerAddress("host", 10000000));
    }
}
