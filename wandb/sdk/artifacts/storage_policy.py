"""Storage policy."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence, NamedTuple

from wandb.sdk.internal.internal_api import Api as InternalApi
from wandb.sdk.lib.paths import FilePathStr, URIStr

if TYPE_CHECKING:
    from concurrent.futures import ThreadPoolExecutor

    from wandb.filesync.step_prepare import StepPrepare
    from wandb.sdk.artifacts.artifact import Artifact
    from wandb.sdk.artifacts.artifact_manifest_entry import ArtifactManifestEntry
    from wandb.sdk.internal.progress import ProgressFn

DEFAULT_THREAD_POOL_SIZE = 64
DEFAULT_PARALLEL_DOWNLOAD_SIZE_BYTES = 1 * 1024 * 1024 * 1024  # 1GB
DEFAULT_CHUNK_SIZE_BYTES = 64 * 1024 * 1024  # 64MB
DEFAULT_HTTP_RESPONSE_CONTENT_ITER_SIZE_BYTES = 1 * 1024 * 1024  # 1MB


class ArtifactDownloadConfig(NamedTuple):
    """Configuration for downloading files in an artifact.

    Attributes:
        thread_pool_size: The size of the thread pool to use for downloading multiple files in artifact or multiple chunks of a single file.
        parallel: Whether to download in parallel. When set to None, decide automatically base on file size. When set to True/False, force parallel/serial regardless of file size.
        file_size_threshold_bytes: The threshold for deciding whether to download in parallel automatically.
        chunk_size_bytes: The size of each chunk to download.
        http_response_content_iter_size_bytes: The size of the response content iterator.
    """

    thread_pool_size: int = DEFAULT_THREAD_POOL_SIZE
    parallel: bool | None = None
    file_size_threshold_bytes: int = DEFAULT_PARALLEL_DOWNLOAD_SIZE_BYTES
    chunk_size_bytes: int = DEFAULT_CHUNK_SIZE_BYTES
    http_response_content_iter_size_bytes: int = (
        DEFAULT_HTTP_RESPONSE_CONTENT_ITER_SIZE_BYTES
    )


DEFAULT_DOWNLOAD_CONFIG = ArtifactDownloadConfig()


class StoragePolicy:
    """Implemented by WandbStoragePolicy."""

    @classmethod
    def lookup_by_name(cls, name: str) -> type[StoragePolicy]:
        import wandb.sdk.artifacts.storage_policies  # noqa: F401

        for sub in cls.__subclasses__():
            if sub.name() == name:
                return sub
        raise NotImplementedError(f"Failed to find storage policy '{name}'")

    @classmethod
    def name(cls) -> str:
        raise NotImplementedError

    @classmethod
    def from_config(cls, config: dict, api: InternalApi | None = None) -> StoragePolicy:
        raise NotImplementedError

    def config(self) -> dict:
        raise NotImplementedError

    def load_file(
        self,
        artifact: Artifact,
        manifest_entry: ArtifactManifestEntry,
        dest_path: str | None = None,
        executor: ThreadPoolExecutor | None = None,
        download_config: ArtifactDownloadConfig = DEFAULT_DOWNLOAD_CONFIG,
    ) -> FilePathStr:
        raise NotImplementedError

    def store_file(
        self,
        artifact_id: str,
        artifact_manifest_id: str,
        entry: ArtifactManifestEntry,
        preparer: StepPrepare,
        progress_callback: ProgressFn | None = None,
    ) -> bool:
        raise NotImplementedError

    def store_reference(
        self,
        artifact: Artifact,
        path: URIStr | FilePathStr,
        name: str | None = None,
        checksum: bool = True,
        max_objects: int | None = None,
    ) -> Sequence[ArtifactManifestEntry]:
        raise NotImplementedError

    def load_reference(
        self,
        manifest_entry: ArtifactManifestEntry,
        local: bool = False,
        dest_path: str | None = None,
    ) -> FilePathStr | URIStr:
        raise NotImplementedError
