import logging
from concurrent.futures import ThreadPoolExecutor

from ceph_to_zfs.jobcontrol import GlobalControl
from ceph_to_zfs.statuslogger import JobLogger, TopLevelLogger

try:
    from flask import Flask
except ModuleNotFoundError:
    print("In order to use the web interface, you must install Flask (`pip install flask` or your distro's equivalent)")


class JobLoggingAdapter(logging.Logger):
    def __init__(self, logger: JobLogger, name, level=logging.NOTSET):
        super().__init__(name, level)
        self.logger = logger

    def handle(self, record):
        if record.exc_text:
            self.logger.log(f'{record.levelname}: {record.msg}\n{record.exc_text}')
        else:
            self.logger.log(f'{record.levelname}: {record.msg}')

    # def log(self, level, msg, *args, exc_info=None, stack_info=False, stacklevel=1, extra=None):
    #     super().log(level, msg, *args, exc_info=exc_info, stack_info=stack_info, stacklevel=stacklevel, extra=extra)


class JobLoggingHandler(logging.Handler):
    def __init__(self, logger: JobLogger, level=logging.NOTSET):
        super().__init__(level)
        self.logger = logger

    def handle(self, record):
        formatted = self.format(record)
        self.logger.log(f'{record.levelname}: {formatted}')


class WebController:
    running: bool = False

    def __init__(self, gbc: GlobalControl, logger: JobLogger):
        self.gbc = gbc
        app = Flask(__name__)
        log_handler = JobLoggingHandler(logger)
        app.logger.handlers.clear()
        app.logger.handlers.append(log_handler)
        wz_logger = logging.getLogger("werkzeug")
        wz_logger.handlers.clear()
        wz_logger.handlers.append(log_handler)
        app.json.sort_keys = False
        self.app: Flask = app
        self.pool = ThreadPoolExecutor(max_workers=16)

        @app.route('/start_all', methods=['GET'])
        def start_all():
            if self.running:
                return "Already running"
            self.running = True
            self.pool.submit(self.run_all)
            return "Started"

        @app.route('/status_simple', methods=['GET'])
        def status():
            return self.format_status_simple(self.gbc.logger)

        @app.route('/test_error', methods=['GET'])
        def test_error():
            raise Exception("Intentional Exception to test error handling")

    def run_all(self):
        self.running = True
        self.gbc.run_all_jobs()
        self.running = False

    def start_web(self):
        self.app.run(host='0.0.0.0', port=9999, threaded=True)

    def format_status_simple(self, item: JobLogger):
        return {
            'name': item.name,
            'status_type': item.status_type.label,
            'status_message': item.status,
            'children': [self.format_status_simple(child) for child in item.children.values()]
        }


def run():
    gbc = GlobalControl.from_config_file('../config.py')
    web_logger = TopLevelLogger('Web Server')
    web = WebController(gbc, web_logger)
    web.start_web()


if __name__ == '__main__':
    run()
