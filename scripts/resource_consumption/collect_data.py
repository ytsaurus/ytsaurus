from yt.wrapper import YtClient
import json
import urllib
import urllib2
import datetime
from collections import defaultdict

class EventMapper(object):
    def nested_dict_find(self, data, path):
        from collections import Iterable
        
        cur_data = data
        path_segments = path.split("/")
        for segment in path_segments:
            if not isinstance(cur_data, Iterable) or not segment in cur_data:
                return 0
            cur_data = cur_data[segment]
        return cur_data
    
    def __call__(self, input_row):
        import json
        from collections import defaultdict
        
        statistics = defaultdict(int)
        if (
            "operation_id" in input_row and "cluster_name" in input_row and
            "event_type" in input_row and input_row["event_type"]
            in ["job_started", "job_completed", "job_aborted", "job_failed", "operation_started"]
        ):
            if "statistics" in input_row and input_row["event_type"] in [
                "job_completed", "job_aborted", "job_failed",
            ]:
                parsed_stat = json.loads(input_row["statistics"])
                statistics["cpu_consumption"] += (
                    self.nested_dict_find(parsed_stat, "user_job/cpu/system/sum") +
                    self.nested_dict_find(parsed_stat, "user_job/cpu/user/sum") +
                    self.nested_dict_find(parsed_stat, "job_proxy/cpu/system/sum") +
                    self.nested_dict_find(parsed_stat, "job_proxy/cpu/user/sum")
                )
                if self.nested_dict_find(parsed_stat, "time/exec/sum"):
                    statistics["total_job_throughput"] += (
                        float(self.nested_dict_find(parsed_stat, "data/input/data_weight/sum")) /
                        self.nested_dict_find(parsed_stat, "time/exec/sum")
                    )
                statistics["input_data_size"] += self.nested_dict_find(
                    parsed_stat, "data/input/compressed_data_size/sum",
                )
                if self.nested_dict_find(parsed_stat, "data/output"):
                    for output_table in self.nested_dict_find(parsed_stat, "data/output").itervalues():
                        statistics["output_data_size"] += self.nested_dict_find(
                            output_table, "compressed_data_size/sum",
                        )
                statistics["job_count"] = 1
                statistics["cumulative_memory_mb_sec"] = self.nested_dict_find(
                    parsed_stat, "user_job/cumulative_memory_mb_sec/sum"
                )
            if "resource_limits" in input_row and input_row["event_type"] == "job_started":
                resource_limits = json.loads(input_row["resource_limits"])
                statistics["requested_cpu"] += resource_limits.get("cpu", 0)
            
            output_row = {
                "operation_id": input_row["operation_id"],
                "authenticated_user": input_row.get("authenticated_user", ""),
                "statistics": statistics,
                "cluster_name": input_row["cluster_name"],
            }
            yield output_row
            
            output_row["cluster_name"] = "all"
            yield output_row


class Reducer(object):
    def __init__(self):
        self.statistics = [
            "cpu_consumption", "requested_cpu",
            "total_job_throughput",
            "input_data_size", "output_data_size", 
            "job_count", "cumulative_memory_mb_sec",
        ]
        
    def __call__(self, key, input_row_iterator):
        from collections import defaultdict
        
        output_row = {
            "statistics": defaultdict(int),
            "children": set(),
            "authenticated_user": "",
            "name": key.get("name", ""),
            "cluster_name": key.get("cluster_name", ""),
        }
        for input_row in input_row_iterator:
            for statistic in self.statistics:
                output_row["statistics"][statistic] += input_row["statistics"].get(statistic, 0)
            if input_row.get("authenticated_user", ""):
                output_row["authenticated_user"] = input_row["authenticated_user"]
            if "children" in input_row:
                output_row["children"] = output_row["children"].union(set(input_row["children"]))
                
        output_row["statistics"] = dict(output_row["statistics"])
        output_row["children"] = list(output_row["children"])
        yield {key: val for key, val in output_row.iteritems() if val}
        

def user_mapper(input_row):
    if "authenticated_user" in input_row:
        yield {"authenticated_user": input_row["authenticated_user"]}

        
def user_reducer(key, input_row_iterator):
    yield {"authenticated_user": key["authenticated_user"]}


def get_data_from_api(url, post_params=None):
    req = urllib2.Request(url)
    req.add_header('Authorization', 'OAuth %s' % "AQAD-qJSJ3AWAAAOLgYhrWt_mUxRnEKnVY4qldw")
    if post_params:
        req.add_data(urllib.urlencode(post_params))
    resp = urllib2.urlopen(req)
    content = resp.read()
    staff = json.loads(content)
    return staff


def get_data_by_chunks(names, url, chunk_size=50, post_params=None):
    chunks = [names[i:i + chunk_size] for i in range(0, len(names), chunk_size)]
    data = []
    for chunk in chunks:
        staff = get_data_from_api(url.format(",".join(chunk), 1000), post_params)
        data += staff.get("result", []) + staff.get("results", [])
    return data


def get_user_services(logins):
    set_of_logins = set(logins)
    persons = []
    total_pages = get_data_from_api(
        "https://abc-back.yandex-team.ru/api/v3/services/members/?page_size=1000"
    )["total_pages"]
    for i in range(total_pages):
        # Здесь из ABC API вычитываются данные про всех пользователей,
        # а не только про тех, которые запускали операции, потому что на момент создания этого кода
        # в данной ручке, не работали фильтры, позволяющие получать информацию
        # про нескольких заданных пользователей за раз, а если делать по одному запросу на пользователя,
        # это роняет API: https://st.yandex-team.ru/TOOLSUP-38440.
        # Теперь это поправили, и можно переписать это место эффективнее,
        # аналогично тому как это делается при работе со staff API,
        # хотя основную часть времени все равно занимают map-reduce операции
        page = get_data_from_api("https://abc-back.yandex-team.ru/api/v3/services/members/?" +
                                 "fields=person.login,service.slug,service.name.en&page_size=1000&page={}".format(i+1))
        for person in page["results"]:
            if person["person"]["login"] in set_of_logins:
                persons.append(person)
    return persons


def get_service_ancestors(logins):
    persons = get_user_services(logins)
    slugs = [person["service"]["slug"] for person in persons]
    services = get_data_by_chunks(
        slugs,
        "https://abc-back.yandex-team.ru/api/v3/services/?slug__in={}&fields=ancestors,name&page_size={}",
    )
    service_info = {service["name"]["en"]: service["ancestors"] for service in services}
    person_info = defaultdict(list)
    for person in persons:
        login = person["person"]["login"]
        service_ancestors = [ancestor["name"]["en"] for ancestor in service_info[person["service"]["name"]["en"]]]
        service_ancestors.append(person["service"]["name"]["en"])
        person_info[login].append(service_ancestors)
    return person_info
    
        
def get_user_info():    
    client = YtClient(proxy="hahn")
    client.config['pickling']['module_filter'] = lambda module: 'hashlib' not in getattr(module, '__name__', '')
    
    rows = client.read_table("//home/dev/ivanashevi/resource_consumption/authenticated_users")
    logins = [row["authenticated_user"] for row in rows]
    post_params = {
        "_fields": ",".join([
            "department_group.department.name.full.en",
            "department_group.ancestors.department.name.full.en",
            "login",
            "official.is_robot",
            "robot_owners",
            "robot_owners.person.login",
            "robot_users",
            "robot_users.person.login",
        ]),
    }
    persons = get_data_by_chunks(
        logins, "https://staff-api.yandex-team.ru/v3/persons?login={}&_limit={}",
        post_params=post_params,
    )
    user_info = {}
    
    robots = filter(lambda person: person["official"]["is_robot"], persons)
    robot_owners = []
    robot_service_ancestors = get_service_ancestors([robot["login"] for robot in robots])
    for robot in robots:
        owners = ([
            owner_data["person"]["login"] for owner_data in
            robot.get("robot_owners", []) + robot.get("robot_users",[])
        ])
        robot_owners += owners
        user_info[robot["login"]] = {
            "owners": owners,
            "is_robot": True,
            "service_ancestors": robot_service_ancestors[robot["login"]]
        }
    
    not_robots = filter(lambda person: not person["official"]["is_robot"], persons)
    not_robots += get_data_by_chunks(
        robot_owners, "https://staff-api.yandex-team.ru/v3/persons?login={}&_limit={}",
        post_params=post_params,
    )
    person_service_ancestors = get_service_ancestors([person["login"] for person in not_robots])
    for person in not_robots:
        department_ancestors = [ancestor["department"]["name"]["full"]["en"]
                                    for ancestor in person["department_group"]["ancestors"]]
        department_ancestors.append(person["department_group"]["department"]["name"]["full"]["en"])
        user_info[person["login"]] = {
            "department_ancestors": [department_ancestors],
            "is_robot": False,
            "service_ancestors": person_service_ancestors.get(person["login"], [])
        }
        
    for login in logins:
        if not login in user_info:
            user_info[login] = {
                "department_ancestors": [["Yandex", "YT Internals"]],
                "is_robot": False,
                "service_ancestors": [["YT Internals"]],
            }
    return user_info

                
class OperationMapper(object):
    def __init__(self, user_info, unit_type):
        self.user_info = user_info
        self.unit_type = unit_type

    def __call__(self, input_row):
        from collections import defaultdict
        
        persons = []
        if input_row.get("authenticated_user", ""):
            if not self.user_info[input_row["authenticated_user"]]["is_robot"] or self.unit_type == "service":
                persons = [input_row["authenticated_user"]]
            else:
                persons = self.user_info[input_row["authenticated_user"]]["owners"]
        
        children = defaultdict(set)
        for person in persons:
            for path in self.user_info[person]["{}_ancestors".format(self.unit_type)]:
                prev = ""
                for unit in reversed(path):
                    if prev:
                        children[unit].add(prev)
                    else:
                        children[unit] = set()
                    prev = unit
        
        for unit, children in children.iteritems():
            yield {
                "name": unit,
                "statistics": input_row["statistics"],
                "cluster_name": input_row["cluster_name"],
                "children": list(children)
            }


def collect_data(start_date, end_date, split_by_cluster=True):
    """
    Aggregate job statistics by services and departments
    Usage example: collect_data("2018-10-08", "2018-10-14") to collect data for one week,
    i.e. to write them into service_info and department_info tables
    """
    client = YtClient(proxy="hahn")
    client.config['pickling']['module_filter'] = lambda module: 'hashlib' not in getattr(module, '__name__', '')
    
    client.run_erase("//home/dev/ivanashevi/resource_consumption/operation_statistics")
    
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    source_tables = []
    while start <= end:
        source_tables.append("//logs/yt-scheduler-log/1d/{}".format(start.date()))
        start += datetime.timedelta(days=1)
    
    client.run_map_reduce(
        mapper=EventMapper(),
        reduce_combiner=Reducer(),
        reducer=Reducer(),
        source_table=source_tables,
        destination_table="<append=true>//home/dev/ivanashevi/resource_consumption/operation_statistics",
        reduce_by=["cluster_name", "operation_id"],
    )
    
    client.run_map_reduce(
        mapper=user_mapper,
        reduce_combiner=user_reducer,
        reducer=user_reducer,
        source_table="//home/dev/ivanashevi/resource_consumption/operation_statistics",
        destination_table="//home/dev/ivanashevi/resource_consumption/authenticated_users",
        reduce_by=["authenticated_user"],
    )
    
    user_info = get_user_info()
    
    client.run_map_reduce(
        mapper=OperationMapper(user_info, "department"),
        reduce_combiner=Reducer(),
        reducer=Reducer(),
        source_table="//home/dev/ivanashevi/resource_consumption/operation_statistics",
        destination_table="//home/dev/ivanashevi/resource_consumption/department_info",
        reduce_by=["cluster_name", "name"],
    )
    
    client.run_map_reduce(
        mapper=OperationMapper(user_info, "service"),
        reduce_combiner=Reducer(),
        reducer=Reducer(),
        source_table="//home/dev/ivanashevi/resource_consumption/operation_statistics",
        destination_table="//home/dev/ivanashevi/resource_consumption/service_info",
        reduce_by=["cluster_name", "name"],
    )
