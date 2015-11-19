import yt.yson as yson

def abs(num):
    if num < 0:
        return -num
    else:
        return num

def load(filename):
    res = {}
    for obj in yson.loads(open(filename).read())["statistics"]:
        res[obj["name"]] = obj["objects_alive"]
    return res

stat1 = load("ref_counted1")
stat2 = load("ref_counted2")
diff = {}

for name in stat1:
    diff[name] = abs(stat1[name] - stat2.get(name, 0))

for name, count in sorted(diff.items(), key=lambda item: -item[1]):
    print name, count
