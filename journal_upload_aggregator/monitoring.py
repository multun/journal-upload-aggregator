from prometheus_client import Summary, Counter, Gauge
from functools import wraps

http_journal_receive_time = Summary(
    "http_journal_upload_time", "Time it took for a journal batch upload to proceed"
)

http_journal_receive_entries_count = Summary(
    "http_journal_upload_entries_count", "Number of entries uploaded in a batch"
)

watchdog_ticks = Counter("watchdog_ticks", "Number of watchdog ticks")

journal_send_rounds = Counter("journal_send_rounds", "Number of ES upload events")

journal_send_exceptions = Counter(
    "journal_send_exceptions", "Number of ES upload exceptions"
)

concurrent_uploads = Gauge("concurrent_uploads", "Number of concurrent ES uploads")


def call_counter(counter):
    def _call_counter(f):
        @wraps(f)
        def _wrapper(*args, **kwargs):
            counter.inc()
            return f(*args, **kwargs)

        return _wrapper

    return _call_counter
