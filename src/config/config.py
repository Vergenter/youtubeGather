from dataclasses import asdict
from typing import Optional, Type, TypeVar

T = TypeVar('T')      # Declare type variable


def read_config(config: dict, model: Type[T]) -> Optional[T]:
    module = config.get(model.__name__, {"state": False})
    if not module.get("state", False):
        return None

    defaults = asdict(model())
    for k, v in defaults.items():
        defaults[k] = module.get(k, v)
    return model(**defaults)
