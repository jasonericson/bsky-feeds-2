import yaml

with open('./config.yml', 'r') as file:
    config_data: dict[str] = yaml.safe_load(file)
HANDLE: str = config_data['handle']
PASSWORD: str = config_data['password']
HOSTNAME: str = config_data['hostname']
DB_PASSWORD: str = config_data['db_password']
FEEDS: dict[str, dict[str, str]] = config_data['feeds']
