"""
Download single large s3 object in parallel using signed URLs.
Based on boto/s3transfer but uses signed URLs without requiring AWS credentials.
"""

import concurrent.futures
import functools
import logging
import math
import os
import queue
import requests
from typing import Optional, Callable

logger = logging.getLogger(__name__)

# Sentinel used to signal completion
SHUTDOWN_SENTINEL = object()

class ShutdownQueue(queue.Queue):
    """A queue that tracks whether it has been shutdown."""
    def __init__(self, maxsize=0):
        queue.Queue.__init__(self, maxsize=maxsize)
        self._shutdown = False

    def get(self, block=True, timeout=None):
        return queue.Queue.get(self, block, timeout)

    def put(self, item, block=True, timeout=None):
        if self._shutdown:
            return
        return queue.Queue.put(self, item, block, timeout)

    def trigger_shutdown(self):
        self._shutdown = True

class MultiDownloader:
    """
    Download single large s3 object in parallel using signed URLs.
    Based on boto/s3transfer but uses signed URLs without requiring AWS credentials.
    """
    def __init__(self, max_concurrency: int = 10, max_io_queue: int = 100, 
                 chunk_size: int = 8 * 1024 * 1024, num_retries: int = 5):
        self._max_concurrency = max_concurrency
        self._max_io_queue = max_io_queue
        self._chunk_size = chunk_size
        self._num_retries = num_retries
        self._session = requests.Session()
        self._ioqueue = ShutdownQueue(self._max_io_queue)

    def download_file(self, url: str, size: int, file_path: str, 
                     callback: Optional[Callable[[int], None]] = None) -> None:
        """
        Download a file from a signed url in parallel.
        
        Args:
            url: The signed URL to download from
            size: Total size of the file in bytes (can be int or str)
            file_path: Local path to save the file to
            callback: Optional callback function that takes bytes transferred as argument
        """
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as controller:
            # 1 thread for managing the parallel downloads
            # 1 thread for managing IO writes
            download_parts_handler = functools.partial(
                self._download_file_as_future,
                url,
                size,
                file_path,
                callback,
            )
            parts_future = controller.submit(download_parts_handler)

            io_writes_handler = functools.partial(
                self._perform_io_writes,
                file_path
            )
            io_future = controller.submit(io_writes_handler)

            results = concurrent.futures.wait(
                [parts_future, io_future],
                return_when=concurrent.futures.FIRST_EXCEPTION,
            )
            self._process_future_results(results)

    def _process_future_results(self, futures):
        finished, unfinished = futures
        for future in finished:
            future.result()

    def _download_file_as_future(self, url: str, size: int, file_path: str, 
                               callback: Optional[Callable[[int], None]]) -> None:
        num_parts = int(math.ceil(size / float(self._chunk_size)))
        download_partial = functools.partial(
            self._download_range,
            url,
            file_path,
            size,
            callback,
        )
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self._max_concurrency) as executor:
                list(executor.map(download_partial, range(num_parts)))
        finally:
            self._ioqueue.put(SHUTDOWN_SENTINEL)

    def _calculate_range_param(self, part_index: int, size: int) -> str:
        start_range = part_index * self._chunk_size
        if part_index == int(math.ceil(size / float(self._chunk_size))) - 1:
            end_range = size - 1
        else:
            end_range = start_range + self._chunk_size - 1
        return f'bytes={start_range}-{end_range}'

    def _download_range(self, url: str, file_path: str, size: int, 
                       callback: Optional[Callable[[int], None]], part_index: int) -> None:
        try:
            range_param = self._calculate_range_param(part_index, size)
            headers = {'Range': range_param}

            last_exception = None
            for i in range(self._num_retries):
                try:
                    response = self._session.get(url, headers=headers, stream=True)
                    response.raise_for_status()
                    
                    current_index = self._chunk_size * part_index
                    for chunk in response.iter_content(chunk_size=16 * 1024):
                        if chunk:
                            self._ioqueue.put((current_index, chunk))
                            current_index += len(chunk)
                            if callback:
                                callback(len(chunk))
                    return
                except (requests.exceptions.RequestException) as e:
                    logger.debug(
                        "Retrying exception caught (%s), "
                        "retrying request, (attempt %s / %s)",
                        e,
                        i + 1,
                        self._num_retries,
                        exc_info=True,
                    )
                    last_exception = e
                    continue
            if last_exception:
                raise last_exception
        finally:
            logger.debug("EXITING _download_range for part: %s", part_index)

    def _perform_io_writes(self, file_path: str) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        with open(file_path, 'wb') as f:
            while True:
                task = self._ioqueue.get()
                if task is SHUTDOWN_SENTINEL:
                    logger.debug(
                        "Shutdown sentinel received in IO handler, "
                        "shutting down IO handler."
                    )
                    return
                else:
                    try:
                        offset, data = task
                        f.seek(offset)
                        f.write(data)
                    except Exception as e:
                        logger.debug(
                            "Caught exception in IO thread: %s",
                            e,
                            exc_info=True,
                        )
                        self._ioqueue.trigger_shutdown()
                        raise