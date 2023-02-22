import yaml
from pathlib import Path

# Todo:: Add class mapping to yaml file


def singleton(cls):
    instance = [None]

    def wrapper(*args, **kwargs):
        if instance[0] is None:
            instance[0] = cls(*args, **kwargs)
        return instance[0]

    return wrapper


# Add Singleton pattern here
@singleton
class YamlReader:
    config: dict = None

    def __init__(self, file_path: str):
        self.config = yaml.safe_load(Path(file_path).read_text())
        print("self.config")

    def get_conf(self):
        return self.config
