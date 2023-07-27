# -*- coding: utf-8 -*-
import yt.wrapper as yt
from flask import Flask, json, request, Response, g, Blueprint
from wtforms import Form, StringField, IntegerField, validators
from gunicorn.app.base import BaseApplication
import logging
import logging.handlers
import os
import copy
import time
from datetime import datetime
from random import randint


api = Blueprint("api", __name__)


def insert_comments(comments_info, new_comment=True):
    g.client.insert_rows("{}/topic_comments".format(g.table_path), comments_info, aggregate=True)
    g.client.insert_rows(
        "{}/user_comments".format(g.table_path), [{
            "user": comment_info["user"],
            "topic_id": comment_info["topic_id"],
            "parent_path": comment_info["parent_path"],
            "update_time": comment_info["update_time"],
        } for comment_info in comments_info],
    )
    g.client.insert_rows(
        "{}/topics".format(g.table_path), [{
            "topic_id": comment_info["topic_id"],
            "comment_count": int(new_comment),
            "update_time": comment_info["update_time"],
        } for comment_info in comments_info],
        aggregate=True,
    )


def find_comment(topic_id, parent_path):
    return list(g.client.lookup_rows(
        "{}/topic_comments".format(g.table_path),
        [{"topic_id": topic_id, "parent_path": parent_path}],
    ))


def log_request_start(request, request_id):
    parameters = request.form.items() if request.method == "POST" else request.args.items()
    param_string = ", ".join(
        ["{}={}".format(name, value) for name, value in parameters if name != "content"]
    )
    logging.info("Request {} from {} started: {} {} with parameters {}".format(
        request_id, request.remote_addr, request.method, request.path, param_string,
    ))


def log_request_completion(message, status):
    logging.info("Request {} completed with code {}: {}".format(g.request_id, status, message))


def make_error_response(message, status):
    log_request_completion(message, status)
    return Response(json.dumps({"error": message}), status=status, mimetype="application/json")


def get_status_code(response_error):
    if response_error.is_concurrent_transaction_lock_conflict():
        return 409
    if response_error.is_request_timed_out():
        return 524
    return 503


def get_guid_field(name, optional=True):
    opt_req_validator = validators.Optional()
    if not optional:
        opt_req_validator = validators.InputRequired(
            "Parameter {} must be specified".format(name)
        )
    return StringField(name, [opt_req_validator, validators.Regexp(
        "-".join("([0-9a-f]{1,8})" for _ in range(4)) + "$",
        message="Guid in parameter {} is incorrect".format(name),
    )])


def get_required_field(name):
    return StringField(name, [validators.InputRequired(
        "Parameter {} must be specified".format(name)
    )])


class PostCommentForm(Form):
    topic_id = get_guid_field("topic_id")
    parent_path = StringField("parent_path")
    user = get_required_field("user")
    content = get_required_field("content")

    def validate(self):
        if not Form.validate(self):
            return False
        if self.topic_id.data and self.parent_path.data == "":
            self.parent_path.errors.append("Parameter parent_path must be specified")
            return False
        return True


class DeleteCommentForm(Form):
    topic_id = get_guid_field("topic_id", optional=False)
    parent_path = get_required_field("parent_path")


class EditCommentForm(DeleteCommentForm):
    content = get_required_field("content")


class TopicCommentsForm(Form):
    topic_id = get_guid_field("topic_id", optional=False)
    parent_path = StringField("parent_path")


class LastTopicsForm(Form):
    from_time = IntegerField("from_time", [validators.Optional()], default=0)
    limit = IntegerField("limit", [validators.Optional()], default=10)


class UserCommentsForm(LastTopicsForm):
    user = get_required_field("user")


class ActiveUsersForm(LastTopicsForm):
    limit = IntegerField("limit", [validators.Optional()], default=10)


def check_parameters(form):
    if not form.validate():
        errors = [
            error for error_messages in form.errors.values()
            for error in error_messages
        ]
        return make_error_response("; ".join(errors), 400)


def get_topic_size(topic_id):
    topic_info = list(g.client.lookup_rows(
        "{}/topics".format(g.table_path), [{"topic_id": topic_id}],
    ))
    if not topic_info:
        return None
    assert len(topic_info) == 1
    return topic_info[0]["comment_count"]


@api.route("/post_comment/", methods=["POST"])
def post_comment():
    form = PostCommentForm(request.form)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    topic_id = form.topic_id.data
    parent_path = form.parent_path.data
    try:
        with g.client.Transaction(type="tablet"):
            new_topic = not topic_id
            if new_topic:
                topic_id = yt.common.generate_uuid()
                comment_id = 0
                parent_id = 0
                parent_path = "0"
            else:
                comment_id = get_topic_size(topic_id)
                if not comment_id:
                    make_error_response("There is no topic with id {}".format(topic_id), 404)

                parent_info = find_comment(topic_id, parent_path)
                if not parent_info:
                    return make_error_response(
                        "There is no comment with parent_path '{}' in topic {}".format(
                            parent_path, topic_id,
                        ), 404,
                    )
                parent_id = parent_info[0]["comment_id"]
                parent_path = "{}/{}".format(parent_path, comment_id)

            creation_time = int(time.mktime(datetime.now().timetuple()))
            insert_comments([{
                "topic_id": topic_id,
                "comment_id": comment_id,
                "parent_id": parent_id,
                "parent_path": parent_path,
                "user": form.user.data,
                "creation_time": creation_time,
                "update_time": creation_time,
                "content": form.content.data,
                "views_count": 0,
                "deleted": False,
            }])
            log_request_completion("Added comment {} in topic {}".format(comment_id, topic_id), 201)

            result = {"comment_id": comment_id, "new_topic": new_topic, "parent_path": parent_path}
            if new_topic:
                result["topic_id"] = topic_id
            return Response(json.dumps(result), status=201, mimetype="application/json")
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))


@api.route("/edit_comment/", methods=["POST"])
def edit_comment():
    form = EditCommentForm(request.form)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    topic_id = form.topic_id.data
    parent_path = form.parent_path.data
    try:
        with g.client.Transaction(type="tablet"):
            comment_info = find_comment(topic_id, parent_path)
            if not comment_info:
                return make_error_response(
                    "There is no comment with parent_path '{}' in topic {}".format(
                        parent_path, topic_id,
                    ), 404,
                )
            assert len(comment_info) == 1
            if comment_info[0]["deleted"]:
                return make_error_response(
                    "Comment with parent_path '{}' in topic {} is deleted".format(parent_path, topic_id), 400,
                )

            cur_time = int(time.mktime(datetime.now().timetuple()))
            comment_info[0]["update_time"] = cur_time
            comment_info[0]["content"] = form.content.data
            comment_info[0]["views_count"] = 0

            insert_comments(comment_info, new_comment=False)
            log_request_completion(
                "Edited comment with parent_path '{}' in topic {}".format(parent_path, topic_id), 200,
            )
            return Response(status=200)
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))


@api.route("/delete_comment/", methods=["POST"])
def delete_comment():
    form = DeleteCommentForm(request.form)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    topic_id = form.topic_id.data
    parent_path = form.parent_path.data
    try:
        with g.client.Transaction(type="tablet"):
            comment_info = find_comment(topic_id, parent_path)
            if not comment_info:
                return make_error_response(
                    "There is no comment with parent_path '{}' in topic {}".format(
                        topic_id, parent_path,
                    ), 404,
                )
            assert len(comment_info) == 1

            cur_time = int(time.mktime(datetime.now().timetuple()))
            comment_info[0]["update_time"] = cur_time
            comment_info[0]["deleted"] = True
            comment_info[0]["views_count"] = 0

            g.client.insert_rows("{}/topic_comments".format(g.table_path), comment_info, aggregate=True)
            g.client.delete_rows(
                "{}/user_comments".format(g.table_path), [{
                    "user": comment_info[0]["user"],
                    "topic_id": comment_info[0]["topic_id"],
                    "parent_path": comment_info[0]["parent_path"],
                }],
            )
            g.client.insert_rows(
                "{}/topics".format(g.table_path), [{
                    "topic_id": topic_id,
                    "update_time": cur_time,
                    "comment_count": 0,
                }], aggregate=True,
            )

            log_request_completion(
                "Deleted comment with parent_path '{}' in topic {}".format(parent_path, topic_id), 200,
            )
            return Response(status=200)
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))


@api.route("/user_comments/", methods=["GET"])
def get_last_user_comments():
    form = UserCommentsForm(request.args)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    user = form.user.data
    limit = form.limit.data
    from_time = form.from_time.data
    try:
        comments_info = list(g.client.select_rows(
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
            limit {3}""".format(g.table_path, user, from_time, limit)
        ))
        log_request_completion("Returned {} comments by user {}".format(len(comments_info), user), 200)
        return Response(json.dumps(comments_info, indent=4), status=200, mimetype="application/json")
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))


@api.route("/topic_comments/", methods=["GET"])
def get_topic_comments():
    form = TopicCommentsForm(request.args)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    topic_id = form.topic_id.data
    parent_path = form.parent_path.data
    try:
        with g.client.Transaction(type="tablet"):
            query = """
                * from [{}/topic_comments] where not deleted and topic_id = '{}'
            """.format(g.table_path, topic_id)

            if parent_path:
                query += """
                    and (parent_path >= '{0}' and parent_path < '{0}0')
                """.format(parent_path)

            query += " order by parent_path asc, creation_time desc limit 1000"

            comments_info = list(g.client.select_rows(query))
            to_insert = copy.deepcopy(comments_info)
            for index, comment_info in enumerate(comments_info):
                to_insert[index]["views_count"] = 1

                comment_info["views_count"] += 1
                comment_info.pop("topic_id")
                comment_info.pop("parent_path")

            g.client.insert_rows("{}/topic_comments".format(g.table_path), to_insert, aggregate=True)
            log_request_completion("Returned {} comments in topic {}".format(len(comments_info), topic_id), 200)
            return Response(json.dumps(comments_info, indent=4), status=200, mimetype="application/json")
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))


@api.route("/last_topics/", methods=["GET"])
def get_last_topics():
    form = LastTopicsForm(request.args)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    limit = form.limit.data
    from_time = form.from_time.data
    try:
        topics_info = list(g.client.select_rows(
            """
            topic_comments.topic_id as topic_id,
            topic_comments.content as content,
            topic_comments.user as user,
            topic_comments.views_count as views_count,
            topics.update_time as update_time
            from [{0}/topics] as topics join [{0}/topic_comments] as topic_comments
            on topics.topic_id = topic_comments.topic_id and topic_comments.parent_path = '0'
            where topics.update_time >= {1} order by topics.update_time desc limit {2}
            """.format(g.table_path, from_time, limit)
        ))
        log_request_completion("Returned {} last topics".format(len(topics_info)), 200)
        return Response(json.dumps(topics_info, indent=4), status=200, mimetype="application/json")
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))


@api.route("/active_users/", methods=["GET"])
def get_active_users():
    form = ActiveUsersForm(request.args)
    error_response = check_parameters(form)
    if error_response:
        return error_response
    limit = form.limit.data

    req = g.yql_client.query(
        """
        select user, count(user) as activity from [{}/user_comments_replica]
         group by user order by activity desc limit {}
        """.format("//home/dt_tutorial/comment_service", limit)
    )
    req.run()

    result = []
    for table in req.get_results():
        table.fetch_full_data()

        for row in table.rows:
            cells = list(row)
            result.append({"user": cells[0], "activity": cells[1]})

    return Response(json.dumps(result, indent=4), status=200, mimetype="application/json")


def make_app():
    import yt_driver_rpc_bindings

    app = Flask(__name__)
    app.register_blueprint(api)

    cluster_name = os.environ["CLUSTER"]
    driver_config = {
        "connection_type": "rpc", "cluster_url": "http://{}.yt.yandex.net".format(cluster_name)
    }
    driver = yt_driver_rpc_bindings.Driver(driver_config)

    logging_config = {
        "rules": [
            {
                "min_level": "info",
                "writers": ["info"],
            }
        ],
        "writers": {
            "info": {
                "file_name": "logs/driver.log",
                "type": "file",
            },
        },
        "watch_period": 1,
    }
    yt_driver_rpc_bindings.configure_logging(logging_config)

    yt.native_driver.lazy_import_driver_bindings(backend_type="rpc", allow_fallback_to_native_driver=True)

    table_path = os.environ["TABLE_PATH"]

    @app.before_request
    def before_request():
        from yql.api.v1.client import YqlClient

        g.yql_client = YqlClient(db=cluster_name, token_path=os.path.expanduser("~/.yql/token"))

        g.client = yt.YtClient(
            cluster_name, config={
                "backend": "rpc",
            },
        )
        yt.config.set_option("_driver", driver, client=g.client)

        g.table_path = table_path
        g.request_id = randint(0, 1000000)

        log_request_start(request, g.request_id)

    return app


class GunicornApp(BaseApplication):
    def __init__(self, host, port, workers):
        self.host = host
        self.port = port
        self.workers = workers
        super(GunicornApp, self).__init__()

    def load_config(self):
        self.cfg.set("workers", self.workers)
        self.cfg.set("bind", "{}:{}".format(self.host, self.port))
        self.cfg.set("timeout", 1000)

    def load(self):
        return make_app()
