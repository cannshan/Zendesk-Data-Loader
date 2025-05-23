import threading
import queue
import time

class ProgressManager:
    def __init__(self, app_instance, total_records, update_interval=500):
        """
        Initialize the ProgressManager.
        
        :param app_instance: Instance of AppAPI to call update_progress_bar
        :param total_records: Total number of records to process for progress calculation
        :param update_interval: Minimum interval between updates in milliseconds (default: 500ms)
        """
        self.app_instance = app_instance  # Reference to AppAPI instance
        self.total_records = total_records
        self.progress_queue = queue.Queue()  # Queue for progress updates
        self.record_count_queue = queue.Queue()  # Queue for record count updates
        self.last_update_time = 0
        self.update_interval = update_interval / 1000  # Convert milliseconds to seconds
        self.running = True
        self.lock = threading.Lock()
        self.thread = None
        self.completed_naturally = False  # Track if operation completed naturally

    def update_progress(self, processed_count, record_count=None):
        """
        Queue a progress update with an optional record count.
        
        :param processed_count: Number of records processed for progress calculation
        :param record_count: Optional total number of records processed (even if not written yet)
        """
        if self.total_records > 0:
            progress = (processed_count / self.total_records) * 100
            self.progress_queue.put(progress)  # Store raw progress, may exceed 100%
            if record_count is not None:
                self.record_count_queue.put(record_count)

    def start(self):
        """Start the progress update loop in a separate thread."""
        if self.running:
            # Initial update with 0 progress and 0 records
            self.app_instance.update_progress_bar(0, 0)
            self.thread = threading.Thread(target=self._process_queue_thread, daemon=True)
            self.thread.start()

    def stop(self, stopped_by_user=False):
        """Stop the progress update loop."""
        self.running = False
        if self.thread:
            self.thread.join()  # Wait for the thread to finish
        if not stopped_by_user and self.completed_naturally:
            # Final update to 100% only if completed naturally, record count unchanged
            self.app_instance.update_progress_bar(100, None)
        # Else, leave progress at the last updated value (capped by frontend)

    def mark_completed(self):
        """Mark the operation as completed naturally."""
        self.completed_naturally = True

    def _process_queue_thread(self):
        """Process queued updates in a separate thread."""
        while self.running:
            current_time = time.time()
            with self.lock:
                if current_time - self.last_update_time >= self.update_interval:
                    try:
                        progress = None
                        record_count = None
                        # Get the latest progress update if available
                        while not self.progress_queue.empty():
                            progress = self.progress_queue.get_nowait()
                        # Get the latest record count if available
                        while not self.record_count_queue.empty():
                            record_count = self.record_count_queue.get_nowait()
                        # Send update if either progress or record_count has changed
                        if progress is not None or record_count is not None:
                            # Send raw progress to frontend, let it cap at 100%
                            self.app_instance.update_progress_bar(progress or 0, record_count)
                            self.last_update_time = current_time
                    except queue.Empty:
                        pass
            time.sleep(0.1)  # Small sleep to prevent tight loop

    def is_running(self):
        """Check if the progress manager is running."""
        return self.running