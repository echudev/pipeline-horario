"""
State management for incremental pipeline processing using Prefect Blocks
"""
from datetime import datetime
from typing import Optional
from functools import lru_cache

from .blocks import PipelineStateBlock


class PipelineState:
    """
    Manages pipeline state persistence for incremental loading using Prefect Blocks.

    This class provides a unified interface that works with both Prefect Cloud blocks
    and local fallback for development/testing.
    """

    def __init__(self, block_name: str = "pipeline-state"):
        """
        Initialize PipelineState with a specific block name.

        Args:
            block_name: Name of the Prefect block to use for state storage
        """
        self._block_name = block_name
        self._block = None

    @property
    def block_name(self) -> str:
        """Get the block name"""
        return self._block_name

    async def get_block(self) -> PipelineStateBlock:
        """Lazy load the Prefect block asynchronously"""
        if self._block is None:
            try:
                # Try to load existing block
                self._block = await PipelineStateBlock.load(self.block_name)
            except Exception:
                # Block doesn't exist, create a new one
                self._block = PipelineStateBlock()
        return self._block

    async def get_last_processed_hour(self) -> Optional[datetime]:
        """
        Get the last processed hour from the Prefect block

        Returns:
            datetime: Last processed hour, or None if no state exists
        """
        block = await self.get_block()
        return block.get_last_processed_hour()

    async def set_last_processed_hour(self, hour: datetime) -> None:
        """
        Save the last processed hour to the Prefect block

        Args:
            hour: The hour that was successfully processed
        """
        block = await self.get_block()
        block.set_last_processed_hour(hour)

    async def get_next_hour_to_process(self) -> datetime:
        """
        Get the next hour that should be processed

        Returns:
            datetime: Next hour to process (last processed + 1 hour, or current hour - 1 if no state)
        """
        block = await self.get_block()
        return block.get_next_hour_to_process()

    async def reset_state(self) -> None:
        """Reset the pipeline state (useful for full reprocessing)"""
        block = await self.get_block()
        block.reset_state()

    async def update_metadata(self, key: str, value) -> None:
        """
        Update metadata in the block

        Args:
            key: Metadata key
            value: Metadata value
        """
        block = await self.get_block()
        block.update_metadata(key, value)

    async def get_metadata(self, key: str, default=None):
        """
        Get metadata value from the block

        Args:
            key: Metadata key
            default: Default value if key doesn't exist

        Returns:
            Metadata value or default
        """
        block = await self.get_block()
        return block.get_metadata(key, default)

    def __repr__(self) -> str:
        return repr(self.block)


@lru_cache()
def get_pipeline_state(block_name: str = "pipeline-state") -> PipelineState:
    """
    Get a cached PipelineState instance

    Args:
        block_name: Name of the Prefect block to use

    Returns:
        PipelineState: Cached instance
    """
    return PipelineState(block_name)
