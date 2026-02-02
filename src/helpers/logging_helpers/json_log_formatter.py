import json
import logging

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, self.datefmt),
            "logger": record.name,
            "severity": record.levelname.lower(),
            "message": record.getMessage(),
        }
        # Include all extra fields (excluding standard logging attrs)
        for key, value in record.__dict__.items():
            if key not in ("name", "msg", "args", "levelname", "levelno",
                           "pathname", "filename", "module", "exc_info",
                           "exc_text", "stack_info", "lineno", "funcName",
                           "created", "msecs", "relativeCreated", "thread",
                           "threadName", "processName", "process"):
                log_record[key] = value

        return json.dumps(log_record)
