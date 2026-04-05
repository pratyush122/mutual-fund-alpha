"""
Main entry point for the Mutual Fund Alpha Decomposition Tool.
"""

import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent / "mutual_fund_alpha" / "src"))

from src.utils.logger import logger
from src.utils.checkpoint import read_checkpoint


def main():
    """Main execution function."""
    logger.info("Starting Mutual Fund Alpha Decomposition Tool")

    # Read checkpoint to see where we left off
    current_checkpoint = read_checkpoint()
    logger.info(f"Current checkpoint: {current_checkpoint}")

    # TODO: Implement the main pipeline logic here
    # This would orchestrate the steps defined in the project requirements

    logger.info("Mutual Fund Alpha Decomposition Tool completed")


if __name__ == "__main__":
    main()
