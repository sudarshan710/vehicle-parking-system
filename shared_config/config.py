import os

APP_NAME = "vehicle_parking_system"

CONFIG_DIR = os.path.join(os.path.expanduser("~"), f".{APP_NAME}")

os.makedirs(CONFIG_DIR, exist_ok=True)

FLAG_PATH = os.path.join(CONFIG_DIR, "flag.flag")
LOG_PATH = os.path.join(CONFIG_DIR, "app.log")

print('inside config file')

def reset_config_contents():
    import os
    for filename in os.listdir(CONFIG_DIR):
        file_path = os.path.join(CONFIG_DIR, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)  
            elif os.path.isdir(file_path):
                import shutil
                shutil.rmtree(file_path)  
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')
