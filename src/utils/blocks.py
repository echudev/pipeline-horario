"""
Prefect Blocks for pipeline state management
"""
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from prefect.blocks.abstract import Block


class PipelineStateBlock(Block):
    """
    A Prefect Block for managing incremental pipeline state in Prefect Cloud.

    This block stores the last processed timestamp and provides methods
    for incremental data processing.
    """

    # Block metadata
    _block_type_name = "Pipeline State Block"
    _block_type_slug = "pipeline-state-block"
    _description = "Manages incremental pipeline state for data processing flows"

    # Block fields
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
        self.last_processed_hour = last_processed_hour
        self.pipeline_version = pipeline_version
        self.metadata = metadata or {}

    def get_last_processed_hour(self) -> Optional[datetime]:
        """
        Get the last processed hour from the block state.

        Returns:
            datetime: Last processed hour, or None if no state exists
        """
        if self.last_processed_hour and self.last_processed_hour.tzinfo is None:
            # Ensure timezone awareness
            self.last_processed_hour = self.last_processed_hour.replace(tzinfo=timezone.utc)
        return self.last_processed_hour

    def set_last_processed_hour(self, hour: datetime) -> None:
        """
        Update the last processed hour in the block.

        Args:
            hour: The hour that was successfully processed
        """
        if hour.tzinfo is None:
            hour = hour.replace(tzinfo=timezone.utc)

        self.last_processed_hour = hour
        # Save the block state immediately
        self.save(name=self._block_document_name, overwrite=True)

    def get_next_hour_to_process(self) -> datetime:
        """
        Get the next hour that should be processed.

        Returns:
            datetime: Next hour to process (last processed + 1 hour, or current hour - 1 if no state)
        """
        last_processed = self.get_last_processed_hour()

        if last_processed is None:
            # If no state exists, process the previous hour
            now = datetime.now(timezone.utc)
            # Round down to the start of the current hour, then go back 1 hour
            current_hour = now.replace(minute=0, second=0, microsecond=0)
            return current_hour.replace(hour=current_hour.hour - 1)
        else:
            # Process the next hour after the last processed
            return last_processed.replace(hour=last_processed.hour + 1)

    def reset_state(self) -> None:
        """Reset the pipeline state (useful for full reprocessing)"""
        self.last_processed_hour = None
        self.save(name=self._block_document_name, overwrite=True)

    def update_metadata(self, key: str, value: Any) -> None:
        """
        Update metadata in the block.

        Args:
            key: Metadata key
            value: Metadata value
        """
        self.metadata[key] = value
        self.save(name=self._block_document_name, overwrite=True)

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
        next_to_process = self.get_next_hour_to_process()
        return f"PipelineStateBlock(last_processed={last_processed}, next_to_process={next_to_process})"
