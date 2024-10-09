from pydantic import BaseSettings


class Config(BaseSettings):
    BROKER_HOST: str
    BROKER_VHOST: str
    BROKER_PORT: str
    BROKER_USER: str
    BROKER_PASS: str
    BROKER_EXCHANGE: str
    BROKER_QUEUE: str
    BROKER_ROUTING: str

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'


app_setting = Config()