package ru.yandex.allotment;

import java.util.List;
import java.util.Map;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.inside.yt.kosher.Yt;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;

public class Allotment {
    public final int index;
    public final int size;
    public final String name;
    public final String medium;
    public Map<Node, List<Location>> nodes = Cf.hashMap(); // node -> {location_id}

    private Integer primaryAllotmentIndex = null;

    public Allotment(String name, Map<String, YTreeNode> attributes) {
        this.name = name;
        this.index = attributes.get("index").intValue();
        this.medium = attributes.get("medium").stringValue();
        this.size = attributes.get("size").intValue();

        if (("default_" + medium).equals(name)) {
            primaryAllotmentIndex = index;
        }
    }

    public boolean isPrimary() {
        return name.equals("default_" + medium);
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

    public void recalculateNodes(List<Node> nodes, Yt yt) {
        this.nodes = Cf.hashMap();
        for (Node node : nodes) {
            List<Location> locations = Cf.arrayList();

            for (Map.Entry<String, Integer> entry : node.allotmentAssignment.entrySet()) {
                if (entry.getValue() == index) {
                    String locationId = entry.getKey();
                    Location location = node.locations.getOrElseApply(locationId, () -> new Location(locationId, index, getPrimaryAllotmentIndex(yt)));
                    locations.add(location);
                }
            }

            if (!locations.isEmpty()) {
                this.nodes.put(node, locations);
            }
        }
    }

    public void printStatus(long now, long warningThreshold) {
        int totalAssigned = 0;
        for (Map.Entry<Node, List<Location>> entry : nodes.entrySet()) {
            totalAssigned += entry.getValue().size();
        }

        int oversize = Math.max(0, totalAssigned - size);
        int missing = Math.max(0, size - totalAssigned);

        Status allotmentAggregatedStatus = Status.OK;

        if (oversize > 0) {
            allotmentAggregatedStatus = Status.WARN;
        }
        if (missing > 0) {
            allotmentAggregatedStatus = Status.ERR;
        }

        for (Map.Entry<Node, List<Location>> entry : nodes.entrySet()) {
            Node node = entry.getKey();
            allotmentAggregatedStatus = allotmentAggregatedStatus.plus(node.getStatus(now, warningThreshold));

            if (node.isGood()) {
                for (Location location : entry.getValue()) {
                    allotmentAggregatedStatus = allotmentAggregatedStatus.plus(location.getStatus());
                }
                // dont check allotments of dead nodes
            }
        }

        String color = ConsoleColors.GREEN;
        if (allotmentAggregatedStatus == Status.WARN) {
            color = ConsoleColors.YELLOW;
        } else if (allotmentAggregatedStatus == Status.ERR) {
            color = ConsoleColors.RED;
        }

        System.out.println(String.format("%sAllotment: '%s', size: '%d', missing: '%d', oversize: '%d',%s", color, name, size, missing, oversize, ConsoleColors.RESET));

        for (Map.Entry<Node, List<Location>> entry : nodes.entrySet()) {
            Node node = entry.getKey();
            color = ConsoleColors.RESET;

            Status nodeAggregatedStatus = node.getStatus(now, warningThreshold);
            if (node.isGood()) {
                for (Location location : entry.getValue()) {
                    nodeAggregatedStatus = nodeAggregatedStatus.plus(location.getStatus());
                }
            }

            if (nodeAggregatedStatus == Status.WARN) {
                color = ConsoleColors.YELLOW;
            } else if (nodeAggregatedStatus == Status.ERR) {
                color = ConsoleColors.RED;
            }

            // |-
            System.out.println(String.format("%s|- Addr: '%s', LastSeen: '%s'%s", color, node.addr, node.formatLastSeenTime(), ConsoleColors.RESET));

            for (Location location : entry.getValue()) {
                color = ConsoleColors.RESET;
                if (node.isGood() && location.isDead) {
                    color = ConsoleColors.RED;
                }
                System.out.println(String.format("%s  |- %s%s", color, location.id, ConsoleColors.RESET));
            }
        }
    }
}
