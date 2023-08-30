# Creating a web service

This section describes an example of creating a simple web service using dynamic tables.

Backend for a small comment storage and display service will be built within the example. You can consider it to be a simplified analog of [Reddit](https://www.reddit.com).
For this purpose, an HTTP-based stateless service will be developed on top of a consistent, fault-tolerant, and scalable repository of topics, comments, and likes.

Technologies and tools used:

- Service source code in Python 3.4 and higher.
- [Replicated dynamic {{product-name}} tables](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md) in synchronous replication mode will be used as a repository. Work with dynamic tables is implemented via the RPC proxy.
- The [Flask](http://flask.pocoo.org/) framework will be used as the basis for the HTTP API.

{% note info "Note" %}

This example does not stipulate the development of the following components: authorization, rate limiting, and service monitorings.
The described example is intended primarily as an introduction to the use and capabilities of dynamic {{product-name}} tables, not for the development of production high-load web services in Python.

{% endnote %}

<!-- Todo: Чтобы скачать примеры кода, перейдите по [ссылке](yt/examples/comment_service). -->

## Designing { #designing }

Service requirements:

- All comments stored in the service must relate to one of the topics.
- Comments within a topic must be organized as a tree. Each comment, except for the root comment of the topic, must have one parent.
- Each comment must be characterized by the `parent_path` string. It is the path from the ancestor IDs of a given comment in the tree.
- The username, number of views, and creation time must be stored for each comment.
- The API must enable you to:
   - Create, edit, and delete comments.
   - Select all topic comments or a subtree of comments within a topic.
   - Select the most recent comments by a given user.
   - Select the most recently added topics.

Expected characteristics:

* Up to 50,000 comments within a topic.
* A typical comment is no more than tens of thousands of characters.
* The service must be horizontally scalable by load (RPS) and data volume.

### API methods { #api }

In case of POST requests, the request parameters are passed in the request body as a JSON document. In GET requests — as an URL. In all cases, the request result or error comes in the response body as a JSON document. Possible HTTP response codes:

| HTTP code | Description |
| -------------- | --------------- |
| 200 | The GET/POST request was successfully complete. |
| 201 | The value was successfully added. |
| 400 | Incorrect request. For example, some mandatory parameters were not set. |
| 404 | The object or objects for a specified criterion were not found. For example, an attempt to add a comment to a non-existing topic. |
| 409 | Conflict when making changes to a dynamic table. |
| 503 | The service is currently unable to process the request. For example, the meta-cluster is updated and/or the dynamic tables were unmounted. |
| 524 | Timeout exceeded when working with dynamic tables. |

In case of any error with 4xx and 5xx codes, a JSON document in the `{"error" : "message"}` form comes in the response body.

Writing data — adding, editing, deleting comments — will be implemented via `POST`, reading — via `GET`. `guid` is used as a comment ID: this approach will ensure an even load on dynamic table shards.

#### Call signatures { #call_signatures }

- `POST: /post_comment`: Adding a comment.

   | Parameter name | Type | Required parameter | Description |
   | -------- | ------ | -------------- | --------------- |
   | `topic_id` | `guid` | No | If the topic ID is not specified, a new topic is created. |
   | `parent_path` | `string` | No | Path to the parent comment. Can be specified if `topic_id` is not specified. |
   | `content` | `string` | Yes | Comment content. |
   | `user` | `string` | Yes | Username. |

If successful, code 201 and a document in the `{"comment_id" : "<guid>", "new_topic" : True | False} ` form are returned.

- `POST: /edit_comment`: Editing a comment.

   | Parameter name | Type | Required parameter | Description |
   | --- | --- | --- | --- |
   | `topic_id` | `guid` | Yes | Topic ID. |
   | `parent_path` | `string` | Yes | Comment path. |
   | `content` | `string` | Yes | New comment content. |

   If successful, code 200 and an empty document are returned.

- `POST: /delete_comment`: Deleting a comment. The comment is not actually deleted from the database in order not not to violate the tree comment structure. Instead, a special flag is set in the comment parameters and it means that the comment was deleted, after which the comment can no longer be edited.

   | Parameter name | Type | Required parameter | Description |
   | --- | --- | --- | --- |
   | `topic_id` | `guid` | Yes | Topic ID. |
   | `parent_path` | `string` | Yes | Comment path. |

   If successful, code 200 and an empty document are returned.

- `GET: /topic_comments`: Getting a comment in the topic. A view counter is stored for each comment and it is incremented by one each time a topic comment is requested.

   | Parameter name | Type | Required parameter | Description |
   | --- | --- | --- | --- |
   | `topic_id` | `guid` | Yes | Topic ID. |
   | `parent_path` | `string` | No | The path to the root comment can be specified if only a subtree of comments for a specified topic is to be obtained. |

   If successful, code 200 and a document with a list of the following comments are returned:

   ```json
   [
     {
       "comment_id":"<id>",
       "parent_id":"<id>",
       "content":"<large_text>",
       "user":"<user_name>",
       "view_count":"<N>",
       "update_time":"<UTC_time>",
       "deleted":"True|False"
     }
   ]
   [{"comment_id" : "<id>", "parent_id" : "<id>", "content" : "<large_text>", "user" : "<user_name>", "view_count" : <N>, "update_time" : "<UTC_time>", "deleted" : True|False}]
   ```

   Returned comments are sorted by `parent_path`, so the responses to the comment come immediately after it and are ordered by creation time.

* `GET: /last_user_comments`: Get a list of user comments sorted in reverse chronological order. It is assumed that there will be a page in the service web interface where the user can view all their comments.

   | Parameter name | Type | Required parameter | Description |
   | --- | --- | --- | --- |
   | `user` | `string` | Yes | Username. |
   | `limit` | `int` | No | Limiting the number of returned comments. |
   | `from_time` | `UTC_time` | No | Return only those comments with `update_time` greater than  this parameter. The `from_time` parameter is specifically used instead of `offset`, because the {{product-name}} query language does not support the `OFFSET` keyword in queries. |

   If successful, code 200 and a document with a list of the following comments are returned:

   ```json
   [{"comment_id" : "<id>", "topic_id" : "<id>", "content" : "<large_text>", "user" : <user_name>, "view_count" : <N>, "update_time" : <UTC_time>}]
   ```

* `GET: /last_topics`: Get the topics in which comments have recently been updated. A topic is a root comment. It is assumed that this request will be used by the homepage of the service and the list will be shared by all users. Pagination support is required.

   | Parameter name | Type | Required parameter | Description |
   | --- | --- | --- | --- |
   | `limit` | `int` | No | Limit on the number of returned topics. |
   | `from_time` | `UTC_time` | No | Return only those comments with `update_time` greater than  this parameter. The `from_time` parameter is specifically used instead of `offset`, because the {{product-name}} query language does not support the `OFFSET` keyword in queries. |

   If successful, code 200 and a document with a list of the following comments are returned:

   ```json
   [{"topic_id" : "<id>", "content" : "<large_text>", "user" = <user_name>, "view_count" : <N>, "update_time" : <UTC_time>}]
   ```

### Description of data tables { #tables_description }

You need to define a required set of tables and their schema. When designing the schema, consider the features and limitations of dynamic tables:

- Each dynamic table has a unique primary key. You can use the primary key to obtain writes by calling the `lookup` command.
- Dynamic tables support transactions, including those between different tables in the `snapshot isolation` model.
- Dynamic tables do not support secondary indexes. Efficient searching or filtering is only possible by the prefix of the primary key using the range output mechanism. Otherwise the request may result in full scan of the table.
- Horizontal scaling of dynamic tables is performed through sharding. A table shard in the system is called a `tablet`. It is specified by a continuous range of primary key values. For sharding to be effective, reading and writing must be evenly distributed across the tablets.
- Secondary indexes can be emulated by creating additional tables by the corresponding primary key. In this case, the master table and the index table must be written to within the same transaction.

The main table needed to store content — `topic_comments` — is needed. Read queries to such a table will be generally limited to a single topic, so we recommend making the topic ID the first key component. You can output consecutive integers as the topic ID or generate them based on the current time. However, this will mean that when new topics are created, they will be given very close numbers, and since new comments are more likely to appear in new topics, the entire writing load will be on one tablet.
Therefore, to ensure that the tablets are loaded evenly when the table is scaled, a randomly generated guid is used as the topic ID. The numbers of comments in creation order are used as the IDs i the topic — this can create close keys, but if the size of the topics is not too large, sharding by topic guid should be sufficient.

The `parent_path` key column also stores the path to the comment in the topic: for the root comment — its id, and `parent_path` of each subsequent comment is derived from `parent_path` of its parent by adding `/` and the parent ID to it. For example, `parent_path` of the comment with the `#id1` ID written in response to the root comment with the `0` ID is written as `0/#id1`. This path arrangement enables you to quickly filter comments in the subtree when calling the `topic_comments` method: if `parent_path` of the root comment in the subtree is the `S` string, `parent_path` of its child will have the `S/#id1/.../#idN` form. It will contain `S` as a prefix. Therefore, if you lexicographically order the comments in the topic by `parent_path`, any subtree will correspond to a continuous block of comments starting from the root comment of the subtree. Thus, assigning `parent_path` as a key column enables you to avoid performing full scan of the table to select comments in the subtree.

The ID of the parent comment is additionally stored for each comment. This is needed to assemble comments into a tree when displaying them. Besides that, the `topic_comments` table stores a comment view counter, for which an aggregation column mechanism is used. This mechanism enables you to increment and decrement the value of a column without reading its previous version.

#### The `topic_comments` table

| Column name | Type | Key column | Description |
| --- | --- | --- | --- |
| `topic_id` | `guid` | Yes | Topic guid. |
| `parent_path` | `string` | Yes | Comment path. |
| `comment_id` | `uint64` | No | Comment ID. Its sequence number when adding to the topic. |
| `parent_id` | `uint64` | No | ID of the comment, in response to which this comment was written. Matches `comment_id` for root comments. |
| `user` | `string` | No | Username of the user who left the comment. |
| `create_time` | `uint64` | No | Comment creation time in POSIX Time. |
| `update_time` | `uint64` | No | The time when the comment was last updated in POSIX Time. |
| `content` | `string` | No | Text of the comment. |
| `views_count` | `int64` | No | Number of views, aggregation column. |
| `deleted` | `boolean` | No | The flag that the comment was deleted. |

To select writes in the `/last_user_comments` call, the `topic_comments` table is not suitable. User comments may be located in different topics, which means that a full scan of the table is required to execute such a query. {{product-name}} does not support secondary indexes, so they also cannot be used.

Therefore, a second, auxiliary `user_comments` table is required where the username is used as the first primary key component. For reasons of even load balancing in future sharding of the table, add a computed column to the beginning of the key — hash `user_name`. The farm_hash function specified in the `expression` field in the table schema (see table creation code) is used for hashing.

#### The `user_comments` table

| Column name | Type | Key column | Description |
| --- | --- | --- | --- |
| `hash(user_name)` | `uint64` | Yes | Computed column. |
| `user_name` | `string` | Yes | Username of the user who left the comment. |
| `topic_id` | `string` | Yes | Topic guid. |
| `parent_path` | `string` | Yes | Comment path. |
| `update_time` | `uint64` | No | The time when the comment was last updated in POSIX Time. |

You need to decide how to process the `/last_topics` call efficiently. A simple way is to query the `topic_comments` table with grouping by `topic_id`. But this approach requires re-running full scan of the largest table. Therefore, it makes sense to create another small `topics` table where `topic_id` will store the last update time of the topic, i.e. any of its comments.

Although this entire table will be viewed, it is much smaller. This will enable you to apply a number of optimizations. For example, upload the entire table into memory or increase the query concurrency by increasing the number of tablets. Moreover, the response to such a query can be cached, because it will be the same for all users. This table will also store the number of comments in the topic so that when a new comment is added, its ID can be obtained.

#### The `topic` table

| Column name | Type | Key column | Description |
| --- | --- | --- | --- |
| `topic_id` | `string` | Yes | Topic guid. |
| `update_time` | `uint64` | No | The time when the comment was last updated in the topic. |
| `comment_count` | `uint64` | No | Number of comments in the topic, including deleted comments. |

## Creating and mounting tables { #tables_preparation }

This example uses synchronous replication with one synchronous and one asynchronous replica and automatic replica mode switching. This mode provides the strictest consistency guarantees as with unreplicated dynamic tables and does not require manual replica mode switching.

Creating a replicated table includes the following steps:

1. Creating a special `replicated_table` object on the meta-cluster.
2. Creating replica tables on other clusters and their corresponding `table_replica` objects on the meta-cluster.

All tables must be created with the same schema. The `ssd_blobs` medium is used to store data. The choice of an SSD medium is driven by the required timings and meta cluster write flow. All replicas will first be created in`async` mode. After the tables are mounted, the synchronous replica will be selected automatically.

The actions described above can be performed either via the CLI — via the `yt create` and `yt set` commands — or via Python wrapper.

For this example, a script was written to create the required tables. This script reads the required parameters in the command line arguments. This enables you to use the script both production and testing environments. Script parameters:

* `meta_cluster`: A cluster with a replicated table.
* `replica_clusters`: Clusters with replicas.
* `path`: Path to the project working directory.
* `force`: A flag used to re-create tables if they already exist. Without it, no changes will be made.

Example of running the script after building it via `ya make`:

```bash
./create_tables --path //path/to/directory/ --meta_cluster <meta_cluster_name> --replica_clusters <replica1-cluster-name> <replica2-cluster-name> --force
```
## Service quotas

Create a dedicated tablet_cell_bundle and an account with an SSD quota. To ensure fault-tolerance of the service, you should have 3 clusters: a meta-cluster and 2 replica clusters. But for training purposes, you can perform all operations on a single cluster. It will act as a meta-cluster and two replicas (synchronous and asynchronous).

## Developing service code { #development }

### Installing the required packages: [Flask](http://flask.pocoo.org/) and [WTForms](https://wtforms.readthedocs.io). WTForms will be used to validate parameters. Installation commands:

```bash
sudo pip install flask
sudo pip install wtforms
```

### Defining the required functions. Environment variables will be used to pass parameters to the program. This approach will then enable the application to be easily moved from the testing environment to the production environment.

The required environment variables:

* `CLUSTER`: The name of the cluster on which replicated tables are stored.
* `TABLE_PATH`: Path to the directory with tables.

Setting the required environment variables:

```bash
export CLUSTER=<cluster-name>
export TABLE_PATH=//path/to/directory
```
Create a function to add comment information to all tables. It can also be used when editing comments and when adding a comment for an example.

### Implementing the `find_comment` call

T function to search for comments in the table takes the `topic_id` and `parent_path` values as arguments. This enables you to quickly find a comment using the `lookup_rows` method, because the values of all key columns of the `topic_comments` table are known.

An example of getting information about the recently added comments:

```python
# -*- coding: utf-8 -*-
import yt.wrapper as yt
import json
import os


def find_comment(topic_id, parent_path, client, table_path):
    # In lookup_rows you need to pass the values of all key columns
    return list(client.lookup_rows(
        "{}/topic_comments".format(table_path),
        [{"topic_id": topic_id, "parent_path": parent_path}],
    ))


def main():
    table_path = os.environ["TABLE_PATH"]
    <cluster-name> = os.environ["CLUSTER"]
    client = yt.YtClient(cluster_name, config={"backend": "rpc"})

    comment_info = find_comment(
        topic_id="1dd64501-4131025-562332a3-40507acc",
        parent_path="0",
        client=client, table_path=table_path,
    )
    print(json.dumps(comment_info, indent=4))


if __name__ == "__main__":
    main()

"""
Program output:

[
    {
        "update_time": 100000,
        "views_count": 0,
        "parent_id": 0,
        "deleted": false,
        "comment_id": 0,
        "creation_time": 100000,
        "content": "Some comment text",
        "parent_path": "0",
        "user": "abc",
        "topic_id": "1dd64501-4131025-562332a3-40507acc"
    }
]
"""
```

### Implementing the `post_comment` call

The first implementation of the `post_comment` call is a function that takes all parameters as arguments and returns a JSON string as a response. The dynamic table queries are gathered into a single transaction. Added exception processing when accessing the {{product-name}} system.

```python
# -*- coding: utf-8 -*-
import yt.wrapper as yt
import os
import json
import time
from datetime import datetime


# Auxiliary function to get the number of comments in a topic from the topic_id table
# If there is no specified topic, None returns
def get_topic_size(client, table_path, topic_id):
    topic_info = list(client.lookup_rows(
        "{}/topics".format(table_path), [{"topic_id": topic_id}],
    ))
    if not topic_info:
        return None
    assert(len(topic_info) == 1)
    return topic_info[0]["comment_count"]


def post_comment(client, table_path, user, content, topic_id=None, parent_id=None):
    # The YtResponseError exception that occurs if the operation fails must be processed
    try:
        # Adding a comment includes several queries of different types
        # and they need to be assembled into a single transaction to ensure atomicity
        with client.Transaction(type="tablet"):
            new_topic = not topic_id
            if new_topic:
                topic_id = yt.common.generate_uuid()
                comment_id = 0
                parent_id = 0
                parent_path = "0"
            else:
                # The comment_id field is set equal to the sequence number of the comment in the topic
                # This number matches the current size of the topic
                comment_id = get_topic_size(topic_id)
                if not comment_id:
                    return json.dumps({"error": "There is no topic with id {}".format(topic_id)})

                parent_info = find_comment(topic_id, parent_path, client, table_path)
                if not parent_info:
                    return json.dumps({"error" : "There is no comment {} in topic {}".format(parent_id, topic_id)})
                parent_id = parent_info[0]["comment_id"]
                parent_path = "{}/{}".format(parent_path, comment_id)

            creation_time = int(time.mktime(datetime.now().timetuple()))
            insert_comments([{
                "topic_id": topic_id,
                "comment_id": comment_id,
                "parent_id": parent_id,
                "parent_path": parent_path,
                "user": user,
                "creation_time": creation_time,
                "update_time": creation_time,
                "content": content,
                "views_count": 0,
                "deleted": False,
            }], client, table_path)

            result = {"comment_id" : comment_id, "new_topic" : new_topic, "parent_path": parent_path}
            if new_topic:
                result["topic_id"] = topic_id
            return json.dumps(result)
    except yt.YtResponseError as error:
        # yt.YtResponseError the __str__ method that returns a detailed error message is defined
        json.dumps({"error" : str(error)})
```

### Implementing the `user_comments` call

The function includes only one `select_rows` query. `user_comments` is used as the main table, because it is sorted by the `user` field and is most effective for retrieving comments of a specified user. An additional `topic_comments` table is connected to this table using `JOIN` to get full information about the comments. In the `ON` section, `topic_id` and `parent_path` must be specified explicitly so that the data from the additional table can be retrieved efficiently by key. The `WHERE` and `LIMIT` sections are used to set the required filters and the `ORDER BY` section is used to specify the order in which the most recent comments must return first.

```python
# -*- coding: utf-8 -*-
import yt.wrapper as yt
import os
import json


def get_last_user_comments(client, table_path, user, limit=10, from_time=0):
    try:
        # user_comments is used as the main table that enables you to filter writes by the user field
        # An additional topic_comments table is connected via join
        # and it enables you to get full information about the omment
        comments_info = list(client.select_rows(
            """
            topic_comments.topic_id as topic_id,
            topic_comments.comment_id as comment_id,
            topic_comments.content as content,
            topic_comments.user as user,
            topic_comments.views_count as views_count,
            topic_comments.update_time as update_time
            from [{0}/user_comments] as user_comments join [{0}/topic_comments] as topic_comments
            on (user_comments.topic_id, user_comments.parent_path) =
            (topic_comments.topic_id, topic_comments.parent_path)
            where user_comments.user = '{1}' and user_comments.update_time >= {2}
            order by user_comments.update_time desc
            limit {3}""".format(table_path, user, from_time, limit)
        ))
        return json.dumps(comments_info, indent=4)
    except yt.YtResponseError as error:
        return json.dumps({"error" : str(error)})
```

Running the application. The application is created via [Blueprint application factory](http://flask.pocoo.org/docs/1.0/blueprints/). A special `Flask.g` object where the application context is configured is used to pass the `client` and `table_path` parameters. The connection settings are passed via the `HOST` and `PORT` environment variables. http://127.0.0.1:5000/ is used by default. Logging is also configured: one message is written to the `comment_service.log` file at the start of the query and another one at the end of the query. The {{product-name}} system debug logs are redirected to the `driver.log` file.

A separate {{product-name}} client must be created for each query so that it is possible to process queries concurrently in the future. An individual driver version is created for each client, which wastes additional resources. To avoid this problem, one common driver object is created at application initialization and then this object is connected to each created client by specifying the corresponding option in the client.

<!-- TODO: Код инициализации и запуска приложения доступен по [ссылке](yt/examples/comment_service/tutorial/fragments/app.py). -->

WTForms forms are used to validate query parameters. For each method, you need to create a separate class form that specifies the query parameters, their type, and other issues.
<!-- TODO: Новая версия метода `post_comment` выглядит [следующим образом](/yt/examples/comment_service/tutorial/fragments/post_comment.py).
TODO: Код метода `user_comments` доступен по [ссылке](/yt/examples/comment_service/tutorial/fragments/user_comments.py). -->

It is assumed that the code is saved in the `run_application.py` file.

```bash
export CLUSTER=<cluster-name>
export TABLE_PATH=//path/to/directory
export HOST=127.0.0.1
export PORT=5000
python run_application.py
```



