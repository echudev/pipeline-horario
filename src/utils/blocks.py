"""
Prefect Blocks for pipeline state management
"""
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from prefect.blocks.abstract import Block

from .datetime_utils import ensure_utc, add_hours, get_previous_hour_start


class PipelineStateBlock(Block):
    """
    A Prefect Block for managing incremental pipeline state in Prefect Cloud.

    This block stores the last processed timestamp and provides methods
    for incremental data processing.
    """

    _block_type_name = "Pipeline State Block"
    _block_type_slug = "pipeline-state-block"
    _description = "Manages incremental pipeline state for data processing flows"

    last_processed_hour: Optional[datetime] = None
    pipeline_version: str = "v1.0.0"
    metadata: Dict[str, Any] = {}

    def __init__(
        self,
        last_processed_hour: Optional[datetime] = None,
        pipeline_version: str = "v1.0.0",
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.last_processed_hour = ensure_utc(last_processed_hour)
        self.pipeline_version = pipeline_version
        self.metadata = metadata or {}

    def get_last_processed_hour(self) -> Optional[datetime]:
        """
        Get the last processed hour from the block state.

        Returns:
            datetime: Last processed hour (UTC), or None if no state exists
        """
        return ensure_utc(self.last_processed_hour)

    def set_last_processed_hour(self, hour: datetime) -> None:
        """
        Update the last processed hour in the block.

        Args:
            hour: The hour that was successfully processed
        
        Raises:
            ValueError: If block document name is not set
        """
        self.last_processed_hour = ensure_utc(hour)
        self._save_block()

    def _save_block(self) -> None:
        """Save block state to Prefect Cloud/Server."""
        if not hasattr(self, '_block_document_name') or not self._block_document_name:
            raise ValueError(
                "Block must be loaded from Prefect before saving. "
                "Use PipelineStateBlock.load('block-name') first."
            )
        self.save(name=self._block_document_name, overwrite=True)

    def get_next_hour_to_process(self) -> datetime:
        """
        Get the next hour that should be processed.

        Returns:
            datetime: Next hour to process (last processed + 1 hour, 
                     or previous hour if no state)
        """
        last_processed = self.get_last_processed_hour()

        if last_processed is None:
            return get_previous_hour_start()
        
        return add_hours(last_processed, 1)

    def reset_state(self) -> None:
        """Reset the pipeline state (useful for full reprocessing)."""
        self.last_processed_hour = None
        self._save_block()

    def update_metadata(self, key: str, value: Any) -> None:
        """
        Update metadata in the block.

        Args:
            key: Metadata key
            value: Metadata value
        """
        self.metadata[key] = value
        self._save_block()

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """
        Get metadata value from the block.

        Args:
            key: Metadata key
            default: Default value if key doesn't exist

        Returns:
            Metadata value or default
        """
        return self.metadata.get(key, default)

    def __repr__(self) -> str:
        last_processed = self.get_last_processed_hour()
        return f"PipelineStateBlock(last_processed={last_processed})"
