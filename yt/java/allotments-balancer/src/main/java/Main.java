import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.bolts.collection.MapF;
import ru.yandex.bolts.collection.Option;
import ru.yandex.inside.yt.kosher.CloseableYt;
import ru.yandex.inside.yt.kosher.Yt;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.YtConfiguration;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeStringNode;

class Allotment {
    public final int index;
    public final String name;
    public final String medium;

    private Integer primaryAllotmentIndex = null;

    public Allotment(String name, Map<String, YTreeNode> attributes) {
        this.name = name;
        this.index = attributes.get("index").intValue();
        this.medium = attributes.get("medium").stringValue();

        if (("default_" + medium).equals(name)) {
            primaryAllotmentIndex = index;
        }
    }

    public int getPrimaryAllotmentIndex(Yt yt) {
        if (primaryAllotmentIndex != null) {
            return primaryAllotmentIndex;
        }

        primaryAllotmentIndex = new Allotment("default_" + medium, yt).index;
        return primaryAllotmentIndex;
    }

    public Allotment(String name, Yt yt) {
        this(name, yt.cypress().get(YPath.simple("//sys/allotments").child(name).allAttributes()).asMap());
    }
}

class Location {
    public final String id;
    public final int allotmentIndex;
    public final int primaryAllotmentIndex;

    public Location(Map<String, YTreeNode> attributes) {
        this.id = attributes.get("location_id").stringValue();
        this.allotmentIndex = attributes.get("allotment_index").intValue();
        this.primaryAllotmentIndex = attributes.get("primary_allotment_index").intValue();
    }
}

class Node {
    public final String addr;

    public final List<Location> locations;

    // location -> index
    public final Map<String, Integer> allotmentAssignment;

    public boolean dirty = false;

    public Node(String addr, Map<String, YTreeNode> attributes) {
        this.addr = addr;
        this.allotmentAssignment = Cf.hashMap();

        Map<String, YTreeNode> allotmentAssignmentRow = attributes.get("allotments_assignment").asMap();

        for (Map.Entry<String, YTreeNode> entity : allotmentAssignmentRow.entrySet()) {
            this.allotmentAssignment.put(entity.getKey(), entity.getValue().intValue());
        }

        this.locations = Cf.arrayList();

        for (YTreeNode locationRow : attributes.get("statistics").asMap().get("locations").asList()) {
            Map<String, YTreeNode> locationAttributes = locationRow.asMap();
            if (!locationAttributes.containsKey("location_id")) {
                continue;
            }

            Location location = new Location(locationAttributes);

            this.locations.add(location);
        }
    }

    public Node(String addr, Yt yt) {
        this(addr, yt.cypress().get(YPath.simple("//sys/nodes").child(addr).allAttributes()).asMap());
    }


    public void assignAllotmentToLocation(String locationId, int allotmentIndex) {
        allotmentAssignment.put(locationId, allotmentIndex);
        dirty = true;
    }

    public void upload(Yt yt, boolean dryRun) {
        if (!dirty) {
            return;
        }

        System.out.println(String.format("%s -> %s", addr, allotmentAssignment));

        if (!dryRun) {
            yt.cypress().set(YPath.simple("//sys/nodes").child(addr).attribute("allotments_assignment"),
                    YTree.builder().value(allotmentAssignment).build());
        }
    }
}

class Main {
    private static class ByNodeTriplet implements Comparable<ByNodeTriplet> {
        int assignment;
        List<String> freeSlots;
        Node node;

        ByNodeTriplet(int assignment, List<String> freeSlots, Node node) {
            this.assignment = assignment;
            this.freeSlots = freeSlots;
            this.node = node;
        }

        @Override
        public int compareTo(ByNodeTriplet byNodeTriplet) {
            return assignment - byNodeTriplet.assignment;
        }
    }

    private static class ByRackTuple implements Comparable<ByRackTuple> {
        int assignment;
        String rack;

        ByRackTuple(int assignment, String rack) {
            this.assignment = assignment;
            this.rack = rack;
        }

        @Override
        public int compareTo(ByRackTuple byRackTuple) {
            return assignment - byRackTuple.assignment;
        }
    }

    public static void assignAllotment(Yt yt, Allotment allotment, int targetSize, boolean dryRun)
    {
        List<Node> nodes = loadNodes(yt);

        MapF<String, PriorityQueue<ByNodeTriplet>> racks = Cf.hashMap();
        PriorityQueue<ByRackTuple> byRack = new PriorityQueue<>();

        int targetPrimaryAllotmentIndex = allotment.getPrimaryAllotmentIndex(yt);

        for (Node node : nodes) {
            List<String> freeSlots = Cf.arrayList();

            for (Location location : node.locations) {
                int currentAssignment = node.allotmentAssignment.getOrDefault(location.id, -1);

                if (currentAssignment == allotment.index) {
                    targetSize -= 1;
                    continue;
                }

                if (currentAssignment != -1) {
                    continue;
                }

                // check medium index
                if (location.primaryAllotmentIndex != targetPrimaryAllotmentIndex) {
                    continue;
                }

                freeSlots.add(location.id);
            }

            if (!freeSlots.isEmpty()) {
                PriorityQueue<ByNodeTriplet> byNode = racks.getOrElseUpdate("allRacks", () -> new PriorityQueue<>());

                byNode.add(new ByNodeTriplet(0, freeSlots, node));
            }
        }

        for (String rack : racks.keySet()) {
            byRack.add(new ByRackTuple(0, rack));
        }

        while (targetSize > 0 && !byRack.isEmpty()) {
            ByRackTuple rackAssignment = byRack.poll();

            PriorityQueue<ByNodeTriplet> byNode = racks.get(rackAssignment.rack);

            ByNodeTriplet nodeAssignment = byNode.poll();

            targetSize -= 1;
            rackAssignment.assignment -= 1;
            nodeAssignment.assignment -= 1;

            String locationId = nodeAssignment.freeSlots.remove(0);

            if (!nodeAssignment.freeSlots.isEmpty()) {
                byNode.add(nodeAssignment);
            }

            if (byNode.isEmpty()) {
                racks.remove(rackAssignment.rack);
            } else {
                byRack.add(rackAssignment);
            }

            nodeAssignment.node.assignAllotmentToLocation(locationId, allotment.index);
        }

        for (Node node : nodes) {
            node.upload(yt, dryRun);
        }
    }

    public static List<Node> loadNodes(Yt yt) {
        List<YTreeStringNode> nodes = yt.cypress().list(YPath.simple("//sys/nodes"));
        List<Node> result = Cf.arrayList();

        for (YTreeStringNode node : nodes) {
            String addr = node.getValue();
            result.add(new Node(addr, yt));
        }

        return result;
    }

    public static void main(String[] args) {
        YtConfiguration.Builder builder = YtConfiguration.builder().withApiHost("socrates.yt.yandex.net");

        CloseableYt yt = Yt.builder(builder.build())
                .http()
                .build();
/*
        List<YTreeStringNode> nodes = yt.cypress().list(YPath.simple("//sys/nodes"));

        for (YTreeStringNode node : nodes) {
            YTreeNode attributes = yt.cypress().get(YPath.simple("//sys/nodes").child(node.getValue()).attribute("allotments_assignment"));
            if (!attributes.mapNode().keys().isEmpty()) {
                System.out.println(attributes.mapNode());
            }
        }
*/

        boolean dryRun = false;
        Allotment allotment = new Allotment("ssd_test_allotment", yt);
        assignAllotment(yt, allotment, 5, dryRun);
        System.exit(0);
    }
}
