# globals.py
import threading

# Global synchronization objects
rate_limit_event = threading.Event()
rate_limit_resume_time = 0
rate_limit_lock = threading.Lock()
rate_limit_event.set()  # Initially unpaused