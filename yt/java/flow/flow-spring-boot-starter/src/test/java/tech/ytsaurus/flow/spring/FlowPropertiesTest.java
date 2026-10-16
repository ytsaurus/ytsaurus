package tech.ytsaurus.flow.spring;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FlowProperties}.
 */
class FlowPropertiesTest {

    @Test
    void defaultPropertiesHaveNullPort() {
        FlowProperties properties = new FlowProperties();
        assertNotNull(properties.getServer());
        assertNull(properties.getServer().getPort());
    }

    @Test
    void portCanBeSet() {
        FlowProperties properties = new FlowProperties();
        properties.getServer().setPort(8080);
        assertEquals(8080, properties.getServer().getPort());
    }

    @Test
    void serverCanBeReplaced() {
        FlowProperties properties = new FlowProperties();
        FlowProperties.Server newServer = new FlowProperties.Server();
        newServer.setPort(9090);
        properties.setServer(newServer);
        assertEquals(9090, properties.getServer().getPort());
    }

    @Test
    void portValidationRejectsZero() {
        FlowProperties properties = new FlowProperties();
        assertThrows(IllegalArgumentException.class, () -> properties.getServer().setPort(0));
    }

    @Test
    void portValidationRejectsNegative() {
        FlowProperties properties = new FlowProperties();
        assertThrows(IllegalArgumentException.class, () -> properties.getServer().setPort(-1));
    }

    @Test
    void portValidationRejectsAboveMax() {
        FlowProperties properties = new FlowProperties();
        assertThrows(IllegalArgumentException.class, () -> properties.getServer().setPort(65536));
    }

    @Test
    void portValidationAcceptsMinPort() {
        FlowProperties properties = new FlowProperties();
        properties.getServer().setPort(1);
        assertEquals(1, properties.getServer().getPort());
    }

    @Test
    void portValidationAcceptsMaxPort() {
        FlowProperties properties = new FlowProperties();
        properties.getServer().setPort(65535);
        assertEquals(65535, properties.getServer().getPort());
    }

    @Test
    void portValidationAcceptsNull() {
        FlowProperties properties = new FlowProperties();
        properties.getServer().setPort(8080);
        properties.getServer().setPort(null);
        assertNull(properties.getServer().getPort());
    }

    @Test
    void isValidReturnsTrueForNullPort() {
        FlowProperties.Server server = new FlowProperties.Server();
        assertTrue(server.isValid());
    }

    @Test
    void isValidReturnsTrueForValidPort() {
        FlowProperties.Server server = new FlowProperties.Server();
        server.setPort(8080);
        assertTrue(server.isValid());
    }

    @Test
    void minPortConstantIsCorrect() {
        assertEquals(1, FlowProperties.Server.MIN_PORT);
    }

    @Test
    void maxPortConstantIsCorrect() {
        assertEquals(65535, FlowProperties.Server.MAX_PORT);
    }
}
