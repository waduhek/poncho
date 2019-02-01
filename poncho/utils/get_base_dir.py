import os


def get_base_dir():
    '''Utility function to get the base directory of the project.

    Returns:
        String containing the base directory of the project.

    Usage:
        Use os.path.join(BASE_DIR, ...) to get absolute path to the specified directory.
    '''
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
