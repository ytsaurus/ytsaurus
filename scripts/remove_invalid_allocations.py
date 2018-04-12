import yt.wrapper as yt

import sys

def main():
    client = yt.YtClient(sys.argv[1])
    
    pods = set([row["meta.id"] for row in client.select_rows("[meta.id] from [//yp/db/pods]")])
    
    updates = []
    allocation_rows = client.select_rows("[meta.id], [status.scheduled_allocations], [status.actual_allocations] from [//yp/db/resources]")
    for row in allocation_rows:
        has_invalid_allocation = False
        for field in ["status.scheduled_allocations", "status.actual_allocations"]:
            good_allocations = []
            if row[field] is None:
                continue
            for allocation in row[field]:
                if allocation["pod_id"] not in pods:
                    has_invalid_allocation = True
                else:
                    good_allocations.append(allocation)
            row[field] = good_allocations
        if has_invalid_allocation:
            updates.append(row)

    print [update["meta.id"] for update in updates]
    #client.insert_rows("//yp/db/resources", updates, update=True)


if __name__ == "__main__":
    main()
