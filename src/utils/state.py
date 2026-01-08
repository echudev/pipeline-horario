"""
State management for incremental pipeline processing using Prefect Blocks.

This module provides async-first state management that persists the last
processed hour to Prefect Cloud/Server blocks for incremental loading.
"""
from datetime import datetime
from typing import Optional
import logging

from .blocks import PipelineStateBlock
from .datetime_utils import ensure_utc, get_previous_hour_start

logger = logging.getLogger(__name__)


class PipelineStateError(Exception):
    """Exception raised when pipeline state operations fail."""
    pass


class PipelineState:
    """
    Manages pipeline state persistence for incremental loading using Prefect Blocks.

    All methods are async to properly interact with Prefect Cloud/Server.
    """

    def __init__(self, block_name: str = "pipeline-state"):
        """
        Initialize PipelineState with a specific block name.

        Args:
            block_name: Name of the Prefect block to use for state storage
        """
        self._block_name = block_name
        self._block: Optional[PipelineStateBlock] = None

    @property
    def block_name(self) -> str:
        """Get the block name."""
        return self._block_name

    async def _load_or_create_block(self) -> PipelineStateBlock:
        """
        Load existing block or create a new one.
        
        Returns:
            PipelineStateBlock instance
        
        Raises:
            PipelineStateError: If block cannot be loaded or created
        """
        if self._block is not None:
            return self._block

        try:
            self._block = await PipelineStateBlock.load(self._block_name)
            logger.info(f"Loaded existing state block: {self._block_name}")
        except ValueError:
            # Block doesn't exist, create new one
            logger.info(f"Creating new state block: {self._block_name}")
            self._block = PipelineStateBlock()
            try:
                await self._block.save(self._block_name, overwrite=False)
                # Reload to get proper block document reference
                self._block = await PipelineStateBlock.load(self._block_name)
            except Exception as e:
                raise PipelineStateError(
                    f"Failed to create state block '{self._block_name}': {e}"
                ) from e
        except Exception as e:
            raise PipelineStateError(
                f"Failed to load state block '{self._block_name}': {e}"
            ) from e

        return self._block

    async def get_last_processed_hour(self) -> Optional[datetime]:
        """
        Get the last processed hour from the Prefect block.

        Returns:
            datetime: Last processed hour (UTC), or None if no state exists
        
        Raises:
            PipelineStateError: If state cannot be retrieved
        """
        block = await self._load_or_create_block()
        return block.get_last_processed_hour()

    async def set_last_processed_hour(self, hour: datetime) -> None:
        """
        Save the last processed hour to the Prefect block.

        Args:
            hour: The hour that was successfully processed
        
        Raises:
            PipelineStateError: If state cannot be saved
        """
        block = await self._load_or_create_block()
        hour = ensure_utc(hour)
        
        try:
            block.last_processed_hour = hour
            await block.save(self._block_name, overwrite=True)
            logger.info(f"State saved: last_processed_hour={hour}")
        except Exception as e:
            raise PipelineStateError(f"Failed to save state: {e}") from e

    async def get_next_hour_to_process(self) -> datetime:
        """
        Get the next hour that should be processed.

        Returns:
            datetime: Next hour to process
        """
        block = await self._load_or_create_block()
        return block.get_next_hour_to_process()

    async def reset_state(self) -> None:
        """
        Reset the pipeline state (useful for full reprocessing).
        
        Raises:
            PipelineStateError: If state cannot be reset
        """
        block = await self._load_or_create_block()
        try:
            block.last_processed_hour = None
            await block.save(self._block_name, overwrite=True)
            logger.info("Pipeline state reset")
        except Exception as e:
            raise PipelineStateError(f"Failed to reset state: {e}") from e

    async def update_metadata(self, key: str, value) -> None:
        """
        Update metadata in the block.

        Args:
            key: Metadata key
            value: Metadata value
        """
        block = await self._load_or_create_block()
        block.metadata[key] = value
        try:
            await block.save(self._block_name, overwrite=True)
        except Exception as e:
            raise PipelineStateError(f"Failed to update metadata: {e}") from e

    async def get_metadata(self, key: str, default=None):
        """
        Get metadata value from the block.

        Args:
            key: Metadata key
            default: Default value if key doesn't exist

        Returns:
            Metadata value or default
        """
        block = await self._load_or_create_block()
        return block.get_metadata(key, default)


# Factory function - returns new instance each time (no caching for async safety)
def get_pipeline_state(block_name: str = "pipeline-state") -> PipelineState:
    """
    Get a PipelineState instance.

    Args:
        block_name: Name of the Prefect block to use

    Returns:
        PipelineState instance
    """
    return PipelineState(block_name)
