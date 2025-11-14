"""
Main entry point for the pipeline
"""
import asyncio
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import get_settings
from pipeline import PipelineOrchestrator


def setup_logging(log_level: str = "INFO"):
    """Configure logging"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


async def main():
    """Main entry point"""
    # Setup logging first with default level
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Get settings (will validate configuration)
        settings = get_settings()
        # Update logging level from settings if available
        setup_logging(settings.LOG_LEVEL)
        logger = logging.getLogger(__name__)
        
        orchestrator = PipelineOrchestrator()
        await orchestrator.run()
    except ValueError as e:
        # Configuration error
        logger.error(f"Configuration error: {e}")
        logger.error("Please set INFLUXDB_TOKEN in your .env file or environment variables")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
