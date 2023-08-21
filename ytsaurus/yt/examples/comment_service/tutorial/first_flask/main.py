# -*- coding: utf-8 -*-
import yt.wrapper as yt
from flask import Flask, json, request, Response, current_app, g, Blueprint
from wtforms import Form, StringField, IntegerField, validators
import logging
import logging.handlers
import os
import copy
import time
from datetime import datetime
from random import randint


# Инициализируем объект Blueprint - "эскиз", на основе которого можно создавать приложения
api = Blueprint("api", __name__)


def insert_comments(comments_info):
    g.client.insert_rows("{}/topic_comments".format(g.table_path), comments_info, aggregate=True)
    g.client.insert_rows(
        "{}/user_comments".format(g.table_path), [{
            "user":  comment_info["user"],
            "topic_id":  comment_info["topic_id"],
            "comment_id": comment_info["comment_id"],
            "update_time": comment_info["update_time"],
        } for comment_info in comments_info],
    )
    g.client.insert_rows(
        "{}/topics".format(g.table_path), [{
            "topic_id":  comment_info["topic_id"],
            "update_time": comment_info["update_time"],
        } for comment_info in comments_info],
    )


def find_comment(topic_id, comment_id):
    return list(g.client.lookup_rows(
        "{}/topic_comments".format(g.table_path),
        [{"topic_id": topic_id, "comment_id": comment_id}],
    ))


# Вспомогательная функция для вывода в лог сообщения о начале запроса
def log_request_start(request, request_id):
    parameters = request.form.items() if request.method == "POST" else request.args.items()
    param_string = ", ".join(
        ["{}={}".format(name, value) for name, value in parameters if name != "content"]
    )
    logging.info("Request {} from {} started: {} {} with parameters {}".format(
        request_id, request.remote_addr, request.method, request.path, param_string,
    ))


# Вспомогательная функция для вывода в лог сообщения о завершении запроса средствами модуля logging
def log_request_completion(message, status):
    logging.info("Request {} completed with code {}: {}".format(g.request_id, status, message))


# Вспомогательная функция для формирования результата неудачного запроса
# Возвращает объект flask.Response, который будет описан ниже
def make_error_response(message, status):
    log_request_completion(message, status)
    return Response(json.dumps({"error": message}), status=status, mimetype="application/json")


# Вспомогательная функция, достающая конкретный код ошибки из исключения yt.YtResponseError
def get_status_code(response_error):
    if response_error.is_concurrent_transaction_lock_conflict():
        return 409
    if response_error.is_request_timed_out():
        return 524
    return 503


# Функция, формирующая поле в форме WTForms для guid комментария, в котором валидируется корректность guid
# а также при необходимости это поле объявляется обязательным
# Поля в WTForms должны создаваться со списком специальных объектов-валидаторов
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


# Функция, формирующая обязательное поле в форме WTForms
def get_required_field(name):
    return StringField(name, [validators.InputRequired(
        "Parameter {} must be specified".format(name)
    )])


# Форма для ручки post_comment
# В WTForms каждая форма должна быть классом, унаследованным от класса wtforms.Form
# Поля задаются как атрибуты этого класса
class PostCommentForm(Form):
    topic_id = get_guid_field("topic_id")
    parent_id = get_guid_field("parent_id")
    user = get_required_field("user")
    content = get_required_field("content")

    def validate(self):
        # Для валидации формы используется метод validate, по умолчанию пробегающий по валидаторам всех полей,
        # и записывающий возникающие ошибки в поле errors этих полей
        # Для данной ручки мы переопределим этот метод, чтобы реализовать чуть более сложную логику:
        # если поле topic_id задано (то есть новый топик не создается), то должен быть задан и parent_id,
        # чтобы знать, где создавать комментарий
        if not Form.validate(self):
            return False
        if self.topic_id.data and not self.parent_id.data:
            self.parent_id.errors.append("Parameter parent_id must be specified")
            return False
        return True


# Форма для ручки user_comments
class UserCommentsForm(Form):
    user = get_required_field("user")
    from_time = IntegerField("from_time", [validators.Optional()], default=0)
    limit = IntegerField("limit", [validators.Optional()], default=10)


# Вспомогательная функция для проверки валидности формы,
# и формирования полного сообщения об ошибке при необходимости
def check_parameters(form):
    if not form.validate():
        errors = [
            error for error_messages in form.errors.values()
            for error in error_messages
        ]
        return make_error_response("; ".join(errors), 400)


# Реализация ручки post_comment
# Ручки создаются с помощью декоратора route, в котором можно задать URL, запускающий функцию и методы запроса
@api.route("/post_comment/", methods=["POST"])
def post_comment():
    # В POST-запросе параметры передаются через объект flask.request.form - это по сути просто словарь,
    # который может быть передан в WTForms
    form = PostCommentForm(request.form)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    # Будем доставать параметры прямо из формы.
    topic_id = form.topic_id.data
    parent_id = form.parent_id.data
    parent_path = form.parent_path.data
    try:
        # Теперь будем передвать параметры как атрибуты объекта g
        # Он будет настраиваться ниже
        with g.client.Transaction(type="tablet"):
            comment_id = yt.common.generate_uuid()
            new_topic = not topic_id
            if new_topic:
                topic_id = comment_id
                parent_id = comment_id
                parent_path = "~"
            else:
                parent_info = find_comment(topic_id, parent_path, parent_id)
                if not parent_info:
                    return make_error_response(
                        "There is no comment {} in topic {} with parent_path {}".format(
                            parent_id, topic_id, parent_path,
                        ), 404,
                    )
                assert(len(parent_info) == 1)
                parent_path = "{}/{}".format(parent_path, parent_id)

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
            # Теперь результатом работы ручки будет объект flask.Response, в котором помимо самого json-результата
            # можно задать HTTP код возврата и тип содержимого
            result = {"comment_id" : comment_id, "new_topic" : new_topic}
            if new_topic:
                result["topic_id"] = topic_id
            return Response(json.dumps(result), status=201, mimetype="application/json")
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))


@api.route("/user_comments/", methods=["GET"])
def get_last_user_comments():
    # В GET-запросе параметры передаются через объект flask.request.args, в остальном все также,
    # как и в предыдущей ручке
    form = UserCommentsForm(request.args)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    user = form.user.data
    limit = form.limit.data
    from_time = form.from_time.data
    try:
        # В качестве основной таблицы используем user_comments, чтобы фильтровать по user,
        # и подключаем дополнительную таблицу topic_comments через join,
        # чтобы получить полную информацию про комментарий
        comments_info = list(g.client.select_rows(
            """
            topic_comments.comment_id as comment_id,
            topic_comments.topic_id as topic_id,
            topic_comments.content as content,
            topic_comments.user as user,
            topic_comments.views_count as views_count,
            topic_comments.update_time as update_time
            from [{0}/user_comments] as user_comments join [{0}/topic_comments] as topic_comments
            on (user_comments.topic_id, user_comments.comment_id) =
            (topic_comments.topic_id, topic_comments.comment_id)
            where user_comments.user = '{1}' and user_comments.update_time >= {2}
            order by user_comments.update_time desc
            limit {3}""".format(g.table_path, user, from_time, limit)
        ))
        log_request_completion("Returned {} comments by user {}".format(len(comments_info), user), 200)
        return Response(json.dumps(comments_info, indent=4), status=200, mimetype="application/json")
    except yt.YtResponseError as error:
        return make_error_response(str(error), status=get_status_code(error))


def make_app():
    import yt_driver_rpc_bindings

    app = Flask(__name__)
    app.register_blueprint(api)

    cluster_name = os.environ["CLUSTER"]
    driver_config = {
        "connection_type": "rpc", "cluster_url": "http://{}.yt.yandex.net".format(cluster_name)
    }
    driver = yt_driver_rpc_bindings.Driver(driver_config)

    # Настраиваем запись в файл logs/driver.log логов драйвера YT.
    # Опция watch_period выполняет ту же функцию, что и WatchedFileHandler
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


def main():
    app = make_app()

    # У flask есть свой собственный лог через логгер werkzeug - отключим его, и будем формировать лог сами
    logger = logging.getLogger('werkzeug')
    logger.setLevel(logging.CRITICAL)

    # Создадим директорию для логов
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Перенаправим лог в файл
    logging.basicConfig(
        filename="logs/comment_service.log", level=logging.DEBUG,
    )

    # Используем WatchedFileHandler - он позволит продолжить писать лог после того,
    # как файл с логами будет создан заново при ротации
    handler = logging.handlers.WatchedFileHandler("logs/comment_service.log")
    handler.setFormatter(logging.Formatter("%(levelname)-8s [%(asctime)s] %(message)s"))
    logging.getLogger().addHandler(handler)

    # Запускаем приложение - после этого к нему можно делать запросы
    # Хост и порт передаем через переменные окружения, или подставляем значения по умолчанию
    app.run(host=os.getenv("HOST", "127.0.0.1"), port=os.getenv("PORT", 5000))


if __name__ == "__main__":
    main()
