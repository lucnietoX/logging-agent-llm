import logging
import logging_loki
from dataflow.constants.constants import LOKI_SERVER

logging_loki.emitter.LokiEmitter.level_tag = "level"


def setup_loki_handler(application_name: str, flow_name: str) -> logging.Logger:
    loki_handler = logging_loki.LokiHandler(
        url=LOKI_SERVER,
        tags={
            "application": application_name,
            "flow": flow_name,
        },
        version="1",
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    loki_handler.setLevel(logging.DEBUG)

    logger = logging.getLogger(flow_name)
    logger.addHandler(loki_handler)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.DEBUG)
    return logger
