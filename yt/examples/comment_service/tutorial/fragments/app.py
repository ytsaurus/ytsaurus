# Инициализация объекта Blueprint - "эскиза", на основе которого создается приложение
api = Blueprint("api", __name__)


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


def main():
    app = make_app()

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

    # Запуск приложения. Для передачи хоста и порта используются переменные окружения
    app.run(host=os.getenv("HOST", "127.0.0.1"), port=os.getenv("PORT", 5000))


if __name__ == "__main__":
    main()
