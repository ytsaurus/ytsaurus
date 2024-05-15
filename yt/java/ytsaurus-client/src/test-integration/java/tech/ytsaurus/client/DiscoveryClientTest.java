package tech.ytsaurus.client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.discovery.Discoverer;
import tech.ytsaurus.client.discovery.GetGroupMeta;
import tech.ytsaurus.client.discovery.GroupMeta;
import tech.ytsaurus.client.discovery.Heartbeat;
import tech.ytsaurus.client.discovery.ListGroups;
import tech.ytsaurus.client.discovery.ListGroupsOptions;
import tech.ytsaurus.client.discovery.ListGroupsResult;
import tech.ytsaurus.client.discovery.ListMembers;
import tech.ytsaurus.client.discovery.ListMembersOptions;
import tech.ytsaurus.client.discovery.MemberInfo;
import tech.ytsaurus.client.discovery.StaticDiscoverer;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class DiscoveryClientTest extends YTsaurusClientTestBase {
    private DiscoveryClient discoveryClient;

    @Before
    public void setup() {
        YTsaurusFixture ytFixture = createYtFixture();
        ytFixture.yt.waitProxies().join();

        YTreeNode node = ytFixture.yt.getNode("//sys/@cluster_connection/discovery_connection/addresses").join();
        Discoverer discoverer = new StaticDiscoverer(node);
        this.discoveryClient = DiscoveryClient.builder()
                .setDiscoverer(discoverer)
                .setOwnBusConnector(new DefaultBusConnector())
                .build();
    }

    private void heartbeat(String groupId, String memberId) {
        Map<String, YTreeNode> attributes = Map.of("attr", YTree.builder().value(memberId + "_v").build());
        Heartbeat req = Heartbeat.builder()
                .setGroupId(groupId)
                .setMemberInfo(new MemberInfo(memberId, 0L, 0L, attributes))
                .setLeaseTimeout(20L * 1000 * 1000)
                .build();
        discoveryClient.heartbeat(req).join();
    }

    @Test
    public void testHeartbeat() {
        heartbeat("/h", "heartbeat");

        ListMembers req = ListMembers.builder()
                .setGroupId("/h").setOptions(new ListMembersOptions(10, List.of("attr"))).build();
        List<MemberInfo> result = discoveryClient.listMembers(req).join().getMembers();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("heartbeat", result.get(0).getId());
        Assert.assertEquals("heartbeat_v", result.get(0).getAttributes().get("attr").stringValue());
    }

    @Test
    public void testListGroups() {
        try {
            discoveryClient.listGroups(ListGroups.builder().setPrefix("/lg").build()).join();
            Assert.fail();
        } catch (CompletionException e) {
            // Must throw "No group" exception
        }

        for (int i = 0; i < 10; i++) {
            heartbeat("/lg/i" + (i % 5), "m");
        }

        ListGroups req1 = ListGroups.builder().setPrefix("/lg").build();
        ListGroupsResult result1 = discoveryClient.listGroups(req1).join();
        Assert.assertEquals(5, result1.getGroupIds().size());
        Assert.assertFalse(result1.isIncomplete());

        ListGroups req2 = ListGroups.builder().setPrefix("/lg").setOptions(new ListGroupsOptions(3)).build();
        ListGroupsResult result2 = discoveryClient.listGroups(req2).join();
        Assert.assertEquals(3, result2.getGroupIds().size());
        Assert.assertTrue(result2.isIncomplete());
    }

    @Test
    public void testGetMeta() {
        for (int i = 0; i < 7; i++) {
            heartbeat("/gm", "m" + i);
        }

        GroupMeta result = discoveryClient.getGroupMeta(GetGroupMeta.builder().setGroupId("/gm").build()).join();
        Assert.assertEquals(7, result.getMemberCount());
    }

    @Test
    public void testListMembers() {
        for (int i = 0; i < 10; i++) {
            heartbeat("/lm", "m" + (i % 4));
        }

        ListMembers req = ListMembers.builder().setGroupId("/lm").build();
        List<MemberInfo> result = discoveryClient.listMembers(req).join().getMembers();
        List<String> ids = result.stream().map(MemberInfo::getId).collect(Collectors.toList());
        Assert.assertEquals(4, ids.size());
        Assert.assertTrue(ids.containsAll(List.of("m0", "m1", "m2", "m3")));
    }
}
