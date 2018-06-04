import yt.wrapper as yt

import sys

def main():
    client = yt.YtClient(sys.argv[1])
    
    pod_to_node = dict([(row["meta.id"], row["spec.node_id"]) for row in client.select_rows("* from [//yp/db/pods]")])
    
    updates = []
    allocation_rows = client.select_rows("* from [//yp/db/resources]")
    for row in allocation_rows:
        resource = row["meta.id"]
        node = row["meta.node_id"]
        has_invalid_allocation = False
        for field in ["status.scheduled_allocations", "status.actual_allocations"]:
            good_allocations = []
            if row[field] is None:
                continue
            for allocation in row[field]:
                if "pod_id" not in allocation:
                    print "Broken allocation {} at node {}".format(allocation, node)
                    has_invalid_allocation = True
                    continue
                pod = allocation["pod_id"]
                if pod not in pod_to_node:
                    print "Resource {} at {} has invalid allocation belonging to nonexistent pod {}".format(resource, node, pod)
                    has_invalid_allocation = True
                elif pod_to_node[pod] != node:
                    print "Resource {} at {} has invalid allocation belonging to pod {} assigned to {}".format(resource, node, pod, pod_to_node[pod])
                    has_invalid_allocation = True
                else:
                    good_allocations.append(allocation)
            row[field] = good_allocations
        if has_invalid_allocation:
            fields_to_del = []
            for field in row:
                if row[field] is None:
                    fields_to_del.append(field)
            for field in fields_to_del:
                del row[field]
            updates.append(row)
    client.insert_rows("//yp/db/resources", updates)

if __name__ == "__main__":
    main()
