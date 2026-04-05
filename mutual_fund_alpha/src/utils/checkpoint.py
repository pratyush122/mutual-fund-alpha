import os


def read_checkpoint(checkpoint_file: str = ".checkpoint") -> str:
    """Read the current checkpoint from file."""
    try:
        with open(checkpoint_file, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def write_checkpoint(step_name: str, checkpoint_file: str = ".checkpoint") -> None:
    """Write the current step name to the checkpoint file."""
    with open(checkpoint_file, "w") as f:
        f.write(step_name)


def clear_checkpoint(checkpoint_file: str = ".checkpoint") -> None:
    """Clear the checkpoint file."""
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
