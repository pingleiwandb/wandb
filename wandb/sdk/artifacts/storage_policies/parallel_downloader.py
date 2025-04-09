# A hand written version of parallel downloader

import concurrent.futures
import functools
import math
import queue
from typing import NamedTuple, Union

import requests

from wandb.sdk.artifacts.artifact_file_cache import Opener

PRAALLEL_DOWNLOAD_SIZE = 1 * 1024 * 1024 * 1024  # 1GB

# TODO: Rename to sentienl? We can also shutdown
_ALL_CHUNK_DOWNLOADED = object()


# TODO: NamedTuple vs TypedDict
class _ChunkContent(NamedTuple):
    offset: int
    data: bytes


class _ChunkQueue(queue.Queue):
    """A blocking queue with no timeout that becomes noop when closed."""

    def __init__(self, maxsize=100):
        super().__init__(maxsize)
        # TODO: Do we need a thread safe version of the flag? It is a best effort to reduce resource usage when there is error
        self._closed = False

    def put(self, item: Union[_ChunkContent, object]):
        if self._closed:
            return
        # TODO: Configure a timeout? It just hangs forever...
        super().put(item, True, None)

    def get(self):
        if self._closed:
            return _ALL_CHUNK_DOWNLOADED
        return super().get(True, None)

    def close(self):
        self._closed = True

    def is_closed(self):
        """If the queue is closed due to previous failure, no need to start new requests."""
        # TODO: Still need to revisit and apply the queue close logic
        return self._closed


class _ChunkRange(NamedTuple):
    start: int
    end: int


def _download_chunk(
    q: _ChunkQueue,
    session: requests.Session,
    singed_url: str,
    range: _ChunkRange,
    response_content_iter_size_bytes: int,
) -> None:
    """Download one chunk base on range."""
    # Skip if the queue is already closed due to exeception
    if q.is_closed():
        return

    headers = {"Range": f"bytes={range.start}-{range.end}"}
    # TODO: catch exception and retry per chunk, I think there is common retry util in SDK
    response = session.get(url=singed_url, headers=headers, stream=True)
    response.raise_for_status()

    file_offset = range.start
    for content in response.iter_content(chunk_size=response_content_iter_size_bytes):
        q.put(_ChunkContent(offset=file_offset, data=content))
        file_offset += len(content)


def _write_chunks_to_file(q: _ChunkQueue, file_opener: Opener) -> None:
    """Write all chunks to disk."""
    with file_opener("wb") as f:
        while True:
            item = q.get()
            if item is _ALL_CHUNK_DOWNLOADED:
                return
            elif isinstance(item, _ChunkContent):
                # TODO: try catch and close the queue if there is io error
                chunk = item
                f.seek(chunk.offset)
                f.write(chunk.data)
            else:
                raise ValueError(f"Unknown queue item type: {type(item)}")


class ParallelDownloader:
    """A parallel downloader using HTTP range requests."""

    def __init__(self):
        pass

    def download_file(
        self,
        executor: concurrent.futures.ThreadPoolExecutor,
        session: requests.Session,
        signed_url: str,
        file_size_bytes: int,
        file_opener: Opener,
        chunk_size_bytes: int = 64 * 1024 * 1024,
        response_content_iter_size_bytes: int = 1024 * 1024,
    ) -> None:
        q = _ChunkQueue(maxsize=100)

        # Start downloadign chunks
        num_chunks = int(math.ceil(file_size_bytes / float(chunk_size_bytes)))
        download_futures = []
        for i in range(num_chunks):
            start = i * chunk_size_bytes
            end = min(start + chunk_size_bytes, file_size_bytes)
            download_handler = functools.partial(
                _download_chunk,
                q,
                session,
                signed_url,
                _ChunkRange(start, end),
                response_content_iter_size_bytes,
            )
            download_futures.append(executor.submit(download_handler))

        # Start writing to file
        write_file_handler = functools.partial(_write_chunks_to_file, q, file_opener)
        write_future = executor.submit(write_file_handler)

        # Wait for download to finish
        done, not_done = concurrent.futures.wait(
            download_futures, return_when=concurrent.futures.FIRST_EXCEPTION
        )
        try:
            for fut in done:
                fut.result()
        finally:
            q.put(_ALL_CHUNK_DOWNLOADED)

        write_future.result()
