import subprocess
import sys

"""
    command for generating requirements.txt 
    pip freeze > requirements.txt
    
    Note:
    the requirements.txt has already been created.
    now to install the necessary packages run below command
    pip install -r requirements.txt
    or run this script.
"""

def install_requirements():
    """Install required Python packages from requirements.txt."""
    try:
        print("Installing dependencies from requirements.txt...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("Dependencies installed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to install dependencies: {e}")
        sys.exit(1)

if __name__ == "__main__":
    install_requirements()
