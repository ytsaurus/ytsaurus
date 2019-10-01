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
    public Map<String, List<String>> nodes = Cf.hashMap(); // node -> {location_id}

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

    public void recalculateNodes(List<Node> nodes) {
        this.nodes = Cf.hashMap();
        for (Node node : nodes) {
            List<String> locations = Cf.arrayList();

            for (Map.Entry<String, Integer> entry : node.allotmentAssignment.entrySet()) {
                if (entry.getValue() == index) {
                    locations.add(entry.getKey());
                }
            }

            if (!locations.isEmpty()) {
                this.nodes.put(node.addr, locations);
            }
        }
    }

    public void printStatus() {
        int totalAssigned = 0;
        for (Map.Entry<String, List<String>> entry : nodes.entrySet()) {
            totalAssigned += entry.getValue().size();
        }

        int oversize = Math.max(0, totalAssigned - size);
        int missing = Math.max(0, size - totalAssigned);
        System.out.println(String.format("Allotment: '%s', size: '%d', missing: '%d', oversize: '%d'", name, size, missing, oversize));

        for (Map.Entry<String, List<String>> entry : nodes.entrySet()) {
            // |-
            System.out.println(String.format("|- %s", entry.getKey()));

            for (String locationId : entry.getValue()) {
                System.out.println(String.format("  |- %s", locationId));
            }
        }
    }
}
