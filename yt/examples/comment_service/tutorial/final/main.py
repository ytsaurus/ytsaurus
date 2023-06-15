# -*- coding: utf-8 -*-
import yt.wrapper as yt
from yql.api.v1.client import YqlClient
from flask import Flask, json, request, Response, current_app, g, Blueprint
from wtforms import Form, StringField, IntegerField, validators
from gunicorn.app.base import BaseApplication
import logging
import logging.handlers
import os
import copy
import time
from datetime import datetime
from random import randint
import sys


# Инициализация объекта Blueprint - "эскиза", на основе которого создается приложение
api = Blueprint("api", __name__)


# В параметре comments_info передаются значения, соответствующие колонкам в таблице topic_comments
def insert_comments(comments_info, new_comment=True):
    # Необходимо указать параметр aggregate=True, без него запись в агрегирующие колонки
    # будет происходить так же, как и в обычные
    g.client.insert_rows("{}/topic_comments".format(g.table_path), comments_info, aggregate=True)
    # При добавлении в таблицу user_comments колонку hash задавать не требуется, она вычислится сама
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
    # В lookup_rows требуется передать значения всех ключевых колонок
    return list(g.client.lookup_rows(
        "{}/topic_comments".format(g.table_path),
        [{"topic_id": topic_id, "parent_path": parent_path}],
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
def make_error_response(message, status):
    log_request_completion(message, status)
    return Response(json.dumps({"error": message}), status=status, mimetype="application/json")


# Вспомогательная функция для получения конкретного кода ошибки из исключения yt.YtResponseError
def get_status_code(response_error):
    if response_error.is_concurrent_transaction_lock_conflict():
        return 409
    if response_error.is_request_timed_out():
        return 524
    return 503


# Функция, формирующая поле в форме WTForms для guid топика, в котором валидируется корректность guid
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


# Форма для метода post_comment
# В WTForms каждая форма должна быть классом, унаследованным от класса wtforms.Form
# Поля формы задаются как атрибуты этого класса
class PostCommentForm(Form):
    topic_id = get_guid_field("topic_id")
    parent_path = StringField("parent_path")
    user = get_required_field("user")
    content = get_required_field("content")

    def validate(self):
        # Для валидации формы используется метод validate, по умолчанию пробегающий по валидаторам всех полей,
        # и записывающий возникающие ошибки в поле errors этих полей
        # В данном случае этот метод переопределяется, чтобы реализовать чуть более сложную логику:
        # если поле topic_id задано (то есть новый топик не создается), то должен быть задан параметр
        # parent_path чтобы знать, где создавать комментарий
        if not Form.validate(self):
            return False
        if self.topic_id.data and self.parent_path.data == "":
            self.parent_path.errors.append("Parameter parent_path must be specified")
            return False
        return True


# Форма для метода delete_comments
class DeleteCommentForm(Form):
    topic_id = get_guid_field("topic_id", optional=False)
    parent_path = get_required_field("parent_path")


# Форма для метода edit_comments
# (она наследуется от DeleteCommentForm, поскольку у этих классов совпадает большинство полей)
class EditCommentForm(DeleteCommentForm):
    content = get_required_field("content")


# Форма для метода topic_comments
class TopicCommentsForm(Form):
    topic_id = get_guid_field("topic_id", optional=False)
    parent_path = StringField("parent_path")


# Форма для метода last_topics
class LastTopicsForm(Form):
    from_time = IntegerField("from_time", [validators.Optional()], default=0)
    limit = IntegerField("limit", [validators.Optional()], default=10)


# Форма для метода user_comments
class UserCommentsForm(LastTopicsForm):
    user = get_required_field("user")


# Форма для ручки active_users
class ActiveUsersForm(LastTopicsForm):
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


# Вспомогательная функция для получения количества комментариев в топике из таблицы topic_id
# В случае отсутствия заданного топика возвращается None
def get_topic_size(topic_id):
    topic_info = list(g.client.lookup_rows(
        "{}/topics".format(g.table_path), [{"topic_id": topic_id}],
    ))
    if not topic_info:
        return None
    assert(len(topic_info) == 1)
    return topic_info[0]["comment_count"]


# Реализация метода post_comment
# Для создания методов API используется декоратор route, в котором можно задается URL, запускающий функцию и методы запроса
@api.route("/post_comment/", methods=["POST"])
def post_comment():
    # В POST-запросе для передачи параметров используется объект типа flask.request.form
    form = PostCommentForm(request.form)
    error_response = check_parameters(form)
    if error_response:
        return error_response

    topic_id = form.topic_id.data
    parent_path = form.parent_path.data
    try:
        # Теперь параметры передаются как атрибуты специального объекта g,
        # который настраивается при создании приложения
        with g.client.Transaction(type="tablet"):
            new_topic = not topic_id
            if new_topic:
                topic_id = yt.common.generate_uuid()
                comment_id = 0
                parent_id = 0
                parent_path = "0"
            else:
                # Поле comment_id задается равным порядковому номеру комментария в топике
                # Этот номер совпадает с текущим размером топика
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

            # Теперь результатом работы метода является объект flask.Response, в котором помимо
            # самого json-результата задается HTTP код возврата и тип содержимого
            result = {"comment_id" : comment_id, "new_topic" : new_topic, "parent_path": parent_path}
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
            assert(len(comment_info) == 1)

            if comment_info[0]["deleted"]:
                return make_error_response(
                    "Comment with parent_path '{}' in topic {} is deleted".format(parent_path, topic_id), 400,
                )

            cur_time = int(time.mktime(datetime.now().timetuple()))
            comment_info[0]["update_time"] = cur_time
            comment_info[0]["content"] = form.content.data
            # Необходимо задать значение агрегирующей колонки равным 0, поскольку это значение прибавится к текущему
            comment_info[0]["views_count"] = 0
            # При редактировании комментария он добавляется заново с новым контентом
            # Поскольку в таблице может находиться не больше одной записи для каждого ключа,
            # данные просто перезаписыавются
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
            assert(len(comment_info) == 1)

            cur_time = int(time.mktime(datetime.now().timetuple()))
            comment_info[0]["update_time"] = cur_time
            comment_info[0]["deleted"] = True
            comment_info[0]["views_count"] = 0
            # Комментарий не удаляется из таблицы topic_comments, для сохранения связности дерева комментариев,
            # вместо этого в нем меняется значение флага deleted на true
            g.client.insert_rows("{}/topic_comments".format(g.table_path), comment_info, aggregate=True)
            # Из таблицы user_comments, комментарий удаляется, что делается с помощью метода delete_rows,
            # куда необходимо послать полный ключ записи о комментарии
            g.client.delete_rows(
                "{}/user_comments".format(g.table_path), [{
                    "user":  comment_info[0]["user"],
                    "topic_id": comment_info[0]["topic_id"],
                    "parent_path": comment_info[0]["parent_path"],
                }],
            )
            # У топика, из которого удаляется комментарий, мы обновляется update_time и comment_count
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
        # В качестве основной таблицы используется таблица user_comments, записи в которой можно фильтровать по полю user
        # Через join подключается дополнительная таблица topic_comments,
        # которая используется для получения полной информации про комментарий
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
            # Для данного достаточно обращения только к таблице topic_comments
            query = """
                * from [{}/topic_comments] where not deleted and topic_id = '{}'
            """.format(g.table_path, topic_id)

            # Проверка принадлежности комментария поддереву. Для этого parent_path комментария должен
            # лексикографически следовать за parent_path корневого комментария, и при этом содержать этот путь
            # в качестве префикса, то есть иметь вид 'parent_path/...'. Первая строка,
            # которая лексикографически больше всех таких строк это 'parent_path0', поскольку символ '0'
            # идет в ascii после '/'
            if parent_path:
                query += """
                    and (parent_path >= '{0}' and parent_path < '{0}0')
                """.format(parent_path)

            query += " order by parent_path asc, creation_time desc limit 1000"

            comments_info = list(g.client.select_rows(query))
            # Необходимо заново добавить комментарии в таблицу, чтобы обновить поле views_count
            to_insert = copy.deepcopy(comments_info)
            for index, comment_info in enumerate(comments_info):
                # В комментариях, добавляемых в таблицы, значение агрегирующей колонки задается равным 1,
                # поскольку это значение будет прибавляться к текущему, а не записываться поверх
                to_insert[index]["views_count"] = 1

                # В комментариях, возвращаемых методом тоже обновляем значение колонки views_count,
                # и убираем колонки, которые этой ручкой возвращать не требуется
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
        # Запрос устроен аналогично запросу, используемому в ручке user_comments,
        # за исключением того, что теперь в качестве основной таблицы используется таблица topics
        # Таблица topic_comments, как и раньше, подключается к при помощи join
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


# Реализация метода active_users
@api.route("/active_users/", methods=["GET"])
def get_active_users():
    form = ActiveUsersForm(request.args)
    error_response = check_parameters(form)
    if error_response:
        return error_response
    limit = form.limit.data

    # К реплицированной таблице нельзя применять map_reduce операции,
    # так что вместо этого используется одна из реплик
    req = g.yql_client.query(
        """
        select user, count(user) as activity from [{}/user_comments_replica]
         group by user order by activity desc limit {}
        """.format("//home/dt_tutorial/comment_service", 3)
    )
    req.run()

    result = []
    for table in req.get_results():
        table.fetch_full_data()

        for row in table.rows:
            cells = list(row)
            result.append({"user": cells[0], "activity": cells[1]})

    return Response(json.dumps(result, indent=4), status=200, mimetype="application/json")


# Функция, создающая конкретное приложение по заданному Blueprint
def make_app():
    import yt_driver_rpc_bindings

    # Создание приложения как объекта Flask
    # Методы API привязываются к приложению с помощью метода register_blueprint
    app = Flask(__name__)
    app.register_blueprint(api)

    # Для каждого запроса следует создать отдельный клиент. Для экономии ресурсов
    # заранее создадается драйвер с требуемой конфигурацией, который потом будет передаваться во все клиенты
    cluster_name = os.environ["CLUSTER"]
    driver_config = {
        "connection_type": "rpc", "cluster_url": "http://{}.yt.yandex.net".format(cluster_name)
    }
    driver = yt_driver_rpc_bindings.Driver(driver_config)

    # Настройка запись в файл logs/driver.log логов драйвера YT.
    # Опция watch_period выполняет ту же функцию, что и WatchedFileHandler
    logging_config = {
        "rules": [
            {
                "min_level": "error",
                "writers": ["error"],
            }
        ],
        "writers": {
            "error": {
                "file_name": "logs/driver.log",
                "type": "file",
            },
        },
        "watch_period": 100,
    }
    yt_driver_rpc_bindings.configure_logging(logging_config)

    # Требуется импортировать модуль с биндингами драйвера с помощью функции lazy_import_driver_bindings,
    # поскольку, когда драйвер создается неявно, данная функция вызывается в коде клиента
    yt.native_driver.lazy_import_driver_bindings(backend_type="rpc", allow_fallback_to_native_driver=True)

    table_path = os.environ["TABLE_PATH"]

    # Метод, который будет вызываться перед основной функцией каждого запроса
    # В данном методе производится настройка объекта g (для каждого запроса он свой)
    # а также в записывается в лог сообщение о начале выполнения запроса
    @app.before_request
    def before_request():
        g.client = yt.YtClient(cluster_name, config={"backend": "rpc"})
        # Подключение ранее созданного драйвера к клиенту
        yt.config.set_option("_driver", driver, client=g.client)
        g.table_path = table_path
        # Запросам присваивается идентификатор, указываемый в логах,
        # что позволяет сопоставить начало и конец каждого запроса
        g.request_id = randint(0, 1000000)
        log_request_start(request, g.request_id)

    return app


# В качестве production сервера используется сервер gunicorn,
# позволяющий разделять нагрузку между несколькими потоками
class GunicornApp(BaseApplication):
    def __init__(self, host, port, workers):
        self.host = host
        self.port = port
        self.workers = workers
        self.timeout = 10
        super(GunicornApp, self).__init__()

    def load_config(self):
        # Параметр workers задает количество потоков, с которыми будет работать сервер
        # Он передается через переменные окружения, как и другие параметры
        self.cfg.set('workers', self.workers)
        self.cfg.set('bind', '{}:{}'.format(self.host, self.port))
        # Может быть полезно увеличить worker timeout (по умолчанию 30 секунд), чтобы запросы
        # не прерывались, если операция занимает много времени
        self.cfg.set("timeout", 1000)

    def load(self):
        return make_app()


def main():
    # Отключение логгера werkzeug, который используется в flask для формирования собственного лога
    logger = logging.getLogger('werkzeug')
    logger.setLevel(logging.CRITICAL)

    # Создание директории для логов
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Перенаправление лога в файл
    logging.basicConfig(
        filename="logs/comment_service.log", level=logging.DEBUG,
    )

    # Подключение к логгеру хэндлера WatchedFileHandler позволяет продолжить продолжить логирование,
    # если файл с логом будет удален, например, в ходе ротации логов
    handler = logging.handlers.WatchedFileHandler("logs/comment_service.log")
    handler.setFormatter(logging.Formatter("%(levelname)-8s [%(asctime)s] %(message)s"))
    logging.getLogger().addHandler(handler)

    app = GunicornApp(
        host=os.getenv("HOST", "127.0.0.1"),
        port=int(os.getenv("PORT", 5000)),
        workers=int(os.environ.get('WORKERS', 10)),
    )
    # Для GunicornApp будет автоматически определен метод run, запускающий приложение
    app.run()


if __name__ == "__main__":
    main()


