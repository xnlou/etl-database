import threading
import time

def periodic_task(task_func, interval, stop_event):
    """Run a task periodically until the stop event is set."""
    def worker():
        while not stop_event.is_set():
            task_func()
            time.sleep(interval)
    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread