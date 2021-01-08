import logging
# import logging.handlers
import os
import sys


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
logging_file_dir = os.path.join(BASE_DIR, "logs")


def create_logger():
    """
    设置日志
    :param app:
    :return:
    """

    # 离线处理更新打印日志
    trace_file_handler = logging.FileHandler(
        os.path.join(logging_file_dir, 'offline.logs')
    )
    trace_file_handler.setFormatter(logging.Formatter('%(message)s'))
    log_trace = logging.getLogger('offline')
    log_trace.addHandler(trace_file_handler)
    log_trace.setLevel(logging.INFO)

    return log_trace