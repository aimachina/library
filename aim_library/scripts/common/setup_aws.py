import os
from utils.configmanager import ConfigManager


def write_config_dict(config, filename, header="[default]"):
    content = []
    content += header + "\n"
    [content.append(f"{k} = {v}\n") for k, v in config.items()]
    with open(filename, "w") as f:
        print(f"Creating file: {filename}")
        f.writelines(content)


def create_config_file(config, filename):
    directory = os.path.dirname(filename)
    if not os.path.exists(directory):
        print(f"Creating directory: {directory}")
        os.makedirs(directory)
    write_config_dict(config, filename)


if __name__ == "__main__":
    credentials_config = ConfigManager.get_config_value("aws", "credentials")
    general_config = ConfigManager.get_config_value("aws", "general")
    create_config_file(credentials_config, credentials_config.pop("path"))
    create_config_file(general_config, general_config.pop("path"))

