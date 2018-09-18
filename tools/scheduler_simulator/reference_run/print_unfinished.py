from yt.wrapper import yson
import json

def show_job_descriptions(desc):
    max_duration = None
    max_memory = None
    max_cpu = None
    max_user_slots = None
    max_network = None
    for d in desc:
        assert len(d) == 8
        max_duration = max(max_duration, d[0])
        max_memory = max(max_memory, d[1])
        max_cpu = max(max_cpu, d[2])
        max_user_slots = max(max_user_slots, d[3])
        max_network = max(max_network, d[4])

    return max_duration, max_memory, max_cpu, max_user_slots, max_network


def main():
    completed = set()
    with open('operations_stats.csv') as fin:
        for line in fin:
            line_splitted = line.split(',')
            completed.add(line_splitted[0])

    all_tags = set()
    with open('operations.yson') as fin:
        a = yson.load(fin, yson_type="list_fragment")
        for item in a:
            if item["operation_id"] in completed:
                continue
            print json.dumps(item, sort_keys=True, indent=4)
            return
            stats = show_job_descriptions(item["job_descriptions"])
            all_tags.add(item["spec"].get("scheduling_tag_filter", "no_filter"))
            print "{}, {}, {}, {}".format(item["operation_id"], item["spec"].get("scheduling_tag_filter", "no_filter"), len(item["job_descriptions"]), stats)
    print 'All tags:', all_tags

if __name__ == '__main__':
    main()
