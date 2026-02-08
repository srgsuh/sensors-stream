import os

class ConfigurationError(Exception):
    pass

class InternalServerError(Exception):
    pass

def get_env_var(var_name: str, default: str | None = None) -> str:
    value = os.getenv(var_name, default)
    if not value:
        raise ConfigurationError(f"Environment variable {var_name} is not set")
    return value

def get_region() -> str:
    return get_env_var("AWS_REGION", "il-central-1")