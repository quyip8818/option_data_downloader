import subprocess
import platform


def open_file_in_application(path):
    if platform.system() == "Darwin":  # macOS
        subprocess.run(["open", path])
    else:  # Linux
        subprocess.run(["xdg-open", path])
