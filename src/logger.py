import logging
import sys
import os

# Configure logging for both local and Lambda environments
def setup_lambda_logging():
    # Check if running in Lambda environment
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        logging.getLogger().setLevel(logging.INFO)
    else:
        # Local environment - configure handlers
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)],
            force=True
        )

# Setup logging
setup_lambda_logging()

def get_logger(name):
    """Get a logger for any module"""
    return logging.getLogger(name)