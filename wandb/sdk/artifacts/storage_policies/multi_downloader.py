"""
Download single large s3 object in parallel using signed URLs.
Based on boto/s3transfer but uses signed URLs without requiring AWS credentials.
"""

import abc
import asyncio
import concurrent.futures
import functools
import logging
import math
import os
import queue
from typing import Optional, Callable, Dict, Any, Union

import requests
import httpx

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

class BaseDownloader(abc.ABC):
    """Base class for all downloader implementations."""
    def __init__(self, max_concurrency: int = 10, max_io_queue: int = 100,
                 iter_content_chunk_size: int = 16 * 1024,
                 chunk_size: int = 8 * 1024 * 1024, num_retries: int = 5):
        self._max_concurrency = max_concurrency
        self._max_io_queue = max_io_queue
        self._chunk_size = chunk_size
        self._num_retries = num_retries
        self._ioqueue = ShutdownQueue(self._max_io_queue)
        self._iter_content_chunk_size = iter_content_chunk_size
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
        num_chunks = int(math.ceil(size / float(self._chunk_size)))
        # Print configuration in human readable format
        print("\nStarting parallel download with configuration:")
        print(f"  - Max concurrent downloads: {self._max_concurrency}")
        print(f"  - Chunk size: {self._chunk_size / (1024*1024):.1f} MB")
        print(f"  - Number of chunks: {num_chunks}")
        print(f"  - IO queue size: {self._max_io_queue}")
        print(f"  - Iter content chunk size: {self._iter_content_chunk_size}")
        print(f"  - Number of retries: {self._num_retries}")
        print(f"  - Total file size: {size / (1024*1024):.1f} MB\n")
        
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

    def _calculate_range_param(self, part_index: int, size: int) -> str:
        start_range = part_index * self._chunk_size
        if part_index == int(math.ceil(size / float(self._chunk_size))) - 1:
            end_range = size - 1
        else:
            end_range = start_range + self._chunk_size - 1
        return f'bytes={start_range}-{end_range}'

    @abc.abstractmethod
    def _download_file_as_future(self, url: str, size: int, file_path: str,
                               callback: Optional[Callable[[int], None]]) -> None:
        """Abstract method to be implemented by specific downloaders."""
        pass

    @abc.abstractmethod
    def _download_range(self, url: str, file_path: str, size: int,
                       callback: Optional[Callable[[int], None]], part_index: int) -> None:
        """Abstract method to be implemented by specific downloaders."""
        pass

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

class RequestsDownloader(BaseDownloader):
    """Original requests-based downloader implementation."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._session = requests.Session()

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
                    for chunk in response.iter_content(chunk_size=self._iter_content_chunk_size):
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

class HttpxDownloaderBase(BaseDownloader):
    """Base class for httpx-based downloaders."""
    def __init__(self, *args, http2: bool = False, **kwargs):
        super().__init__(*args, **kwargs)
        self._http2 = http2
        self._client = self._create_client()

    def _create_client(self) -> httpx.Client:
        return httpx.Client(http2=self._http2)

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

    def _download_range(self, url: str, file_path: str, size: int,
                       callback: Optional[Callable[[int], None]], part_index: int) -> None:
        try:
            range_param = self._calculate_range_param(part_index, size)
            headers = {'Range': range_param}

            last_exception = None
            for i in range(self._num_retries):
                try:
                    with self._client.stream('GET', url, headers=headers) as response:
                        response.raise_for_status()
                        current_index = self._chunk_size * part_index
                        for chunk in response.iter_bytes(chunk_size=self._iter_content_chunk_size):
                            if chunk:
                                self._ioqueue.put((current_index, chunk))
                                current_index += len(chunk)
                                if callback:
                                    callback(len(chunk))
                        return
                except httpx.HTTPError as e:
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

class HttpxSyncDownloader(HttpxDownloaderBase):
    """Httpx-based downloader with synchronous I/O."""
    pass

class HttpxAsyncDownloader(BaseDownloader):
    """Httpx-based downloader with async I/O and HTTP/2 support."""
    def __init__(self, *args, http2: bool = True, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = None
        self._http2 = http2

    def _download_file_as_future(self, url: str, size: int, file_path: str,
                               callback: Optional[Callable[[int], None]]) -> None:
        """
        Implement abstract method but not used - we override download_file instead.
        This is just to satisfy the abstract base class requirement.
        """
        raise NotImplementedError(
            "This method should not be called - HttpxAsyncDownloader uses _async_download_file instead"
        )

    def _download_range(self, url: str, file_path: str, size: int,
                       callback: Optional[Callable[[int], None]], part_index: int) -> None:
        """
        Implement abstract method but not used - we use async version instead.
        This is just to satisfy the abstract base class requirement.
        """
        raise NotImplementedError(
            "This method should not be called - HttpxAsyncDownloader uses async _download_range instead"
        )

    async def _create_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(http2=self._http2)

    def download_file(self, url: str, size: int, file_path: str,
                     callback: Optional[Callable[[int], None]] = None) -> None:
        """Override to run with asyncio event loop."""
        asyncio.run(self._async_download_file(url, size, file_path, callback))

    async def _async_download_file(self, url: str, size: int, file_path: str,
                                 callback: Optional[Callable[[int], None]]) -> None:
        num_chunks = int(math.ceil(size / float(self._chunk_size)))
        print("\nStarting parallel download with configuration:")
        print(f"  - Max concurrent downloads: {self._max_concurrency}")
        print(f"  - Chunk size: {self._chunk_size / (1024*1024):.1f} MB")
        print(f"  - Number of chunks: {num_chunks}")
        print(f"  - IO queue size: {self._max_io_queue}")
        print(f"  - Number of retries: {self._num_retries}")
        print(f"  - Total file size: {size / (1024*1024):.1f} MB\n")

        async with await self._create_client() as client:
            self._client = client
            download_task = asyncio.create_task(self._download_chunks(url, size, file_path, callback))
            io_task = asyncio.create_task(self._async_perform_io_writes(file_path))
            
            await asyncio.gather(download_task, io_task)

    async def _download_chunks(self, url: str, size: int, file_path: str,
                             callback: Optional[Callable[[int], None]]) -> None:
        num_parts = int(math.ceil(size / float(self._chunk_size)))
        tasks = []
        sem = asyncio.Semaphore(self._max_concurrency)
        
        for part_index in range(num_parts):
            task = asyncio.create_task(
                self._download_range(url, file_path, size, callback, part_index, sem)
            )
            tasks.append(task)
        
        try:
            await asyncio.gather(*tasks)
        finally:
            await self._ioqueue.put(SHUTDOWN_SENTINEL)

    async def _download_range(self, url: str, file_path: str, size: int,
                            callback: Optional[Callable[[int], None]], part_index: int,
                            sem: asyncio.Semaphore) -> None:
        async with sem:
            try:
                range_param = self._calculate_range_param(part_index, size)
                headers = {'Range': range_param}

                last_exception = None
                for i in range(self._num_retries):
                    try:
                        async with self._client.stream('GET', url, headers=headers) as response:
                            response.raise_for_status()
                            current_index = self._chunk_size * part_index
                            async for chunk in response.aiter_bytes(chunk_size=self._iter_content_chunk_size):
                                if chunk:
                                    await self._ioqueue.put((current_index, chunk))
                                    current_index += len(chunk)
                                    if callback:
                                        callback(len(chunk))
                            return
                    except httpx.HTTPError as e:
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

    async def _async_perform_io_writes(self, file_path: str) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(file_path)), exist_ok=True)
        with open(file_path, 'wb') as f:
            while True:
                task = await self._ioqueue.get()
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
