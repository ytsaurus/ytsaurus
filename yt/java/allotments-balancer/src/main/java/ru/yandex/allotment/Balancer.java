package ru.yandex.allotment;

import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import ru.yandex.bolts.collection.Cf;
import ru.yandex.bolts.collection.MapF;
import ru.yandex.inside.yt.kosher.Yt;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.transactions.utils.YtTransactionsUtils;
import ru.yandex.inside.yt.kosher.ytree.YTreeStringNode;

class ByNodeTriplet implements Comparable<ByNodeTriplet> {
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

class ByRackTuple implements Comparable<ByRackTuple> {
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

public class Balancer {
    private final Yt yt;
    private List<Node> nodes;
    private Map<String, Allotment> allotments;

    private final long warningThreshold = 0; // 300; // seconds

    public Balancer(Yt yt) {
        this.yt = yt;
        this.nodes = loadNodes();
        this.allotments = loadAllotments();
    }

    public void reassignAllotment(String allotmentName, boolean dryRun) {
        if (!allotments.containsKey(allotmentName)) {
            return;
        }

        Allotment allotment = allotments.get(allotmentName);
        reassignAllotment(allotment);

        // TODO: execute
    }

    private boolean reassignAllotment(Allotment allotment)
    {
        int targetSize = allotment.size;
        long now = System.currentTimeMillis() / 1000;

        MapF<String, PriorityQueue<ByNodeTriplet>> racks = Cf.hashMap();
        PriorityQueue<ByRackTuple> byRack = new PriorityQueue<>();
        Map<String, Integer> initialAssignmentsOfRacks = Cf.hashMap();

        int targetPrimaryAllotmentIndex = allotment.getPrimaryAllotmentIndex(yt);

        for (Node node : nodes) {
            List<String> freeSlots = Cf.arrayList();

            if (node.getStatus(now, warningThreshold) == Status.ERR) {
                continue;
            }

            if (node.isGood() && !node.locations.isEmpty()) {
                node.removeDeadLocations(allotment);
            }

            if (node.isGood()) {
                for (Map.Entry<String, Location> entry : node.locations.entrySet()) {
                    Location location = entry.getValue();

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
            } else {
                // skip locations on partialy dead node

                for (Map.Entry<String, Integer> entry : node.allotmentAssignment.entrySet()) {
                    if (entry.getValue() == allotment.index) {
                        targetSize -= 1;
                    }
                }
            }

            if (!freeSlots.isEmpty()) {
                // TODO: allRacks -> realRack
                String rackName = "allRacks";

                PriorityQueue<ByNodeTriplet> byNode = racks.getOrElseUpdate(rackName, () -> new PriorityQueue<>());

                int assignment = node.locationsOfAllotment(allotment);

                int assignmentByRack = initialAssignmentsOfRacks.getOrDefault(rackName, 0);
                initialAssignmentsOfRacks.put(rackName, assignmentByRack + assignment);

                byNode.add(new ByNodeTriplet(assignment, freeSlots, node));
            }
        }

        for (String rack : racks.keySet()) {
            byRack.add(new ByRackTuple(initialAssignmentsOfRacks.get(rack), rack));
        }

        while (targetSize > 0 && !byRack.isEmpty()) {
            ByRackTuple rackAssignment = byRack.poll();

            PriorityQueue<ByNodeTriplet> byNode = racks.get(rackAssignment.rack);

            ByNodeTriplet nodeAssignment = byNode.poll();

            targetSize -= 1;
            rackAssignment.assignment += 1;
            nodeAssignment.assignment += 1;

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

        boolean dirty = false;
        for (Node node : nodes) {
            dirty |= node.dirty;
        }

        if (!dirty && targetSize == 0) {
            // safe remove dead nodes
            for (Node node : nodes) {
                if (node.getStatus(now, warningThreshold) == Status.ERR) {
                    dirty |= node.moveOut(allotment);
                }
            }
        }

        if (dirty) {
            System.out.println(String.format("Reassigning: Allotment: '%s', TargetSize: '%d'", allotment.name, allotment.size));
        }

        return dirty;
    }

    private Map<String, Allotment> loadAllotments() {
        List<YTreeStringNode> allotments = yt.cypress().list(YPath.simple("//sys/allotments"));
        Map<String, Allotment> result = Cf.hashMap();

        for (YTreeStringNode node : allotments) {
            Allotment allotment = new Allotment(node.getValue(), yt);
            if (!allotment.isPrimary()) {
                allotment.recalculateNodes(this.nodes, yt);
                result.put(allotment.name, allotment);
            }
        }

        return result;
    }

    private List<Node> loadNodes() {
        List<YTreeStringNode> nodes = yt.cypress().list(YPath.simple("//sys/nodes"));
        List<Node> result = Cf.arrayList();

        for (YTreeStringNode node : nodes) {
            try {
                String addr = node.getValue();
                result.add(new Node(addr, yt));
            } catch (Exception ex) {
                // TODO: Warn message
            }
        }

        return result;
    }

    public void printStatus() {
        long now = System.currentTimeMillis() / 1000;

        for (Map.Entry<String, Allotment> entry : allotments.entrySet()) {
            Allotment allotment = entry.getValue();
            allotment.printStatus(now, warningThreshold);
        }
    }

    public void balance(boolean dryRun) {
        boolean dirty = false;
        for (Map.Entry<String, Allotment> entry : allotments.entrySet()) {
            dirty |= reassignAllotment(entry.getValue());
        }

        if (dirty) {
            YtTransactionsUtils.withTransaction(yt, (tx) -> {
                for (Node node : nodes) {
                    node.upload(yt, tx.getId(), dryRun);
                }
            });

            this.nodes = loadNodes();

            for (Map.Entry<String, Allotment> entry : allotments.entrySet()) {
                entry.getValue().recalculateNodes(nodes, yt);
            }
        }
    }
}
