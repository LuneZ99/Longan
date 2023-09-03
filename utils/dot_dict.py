import yaml
from typing import Any


class DotDict(dict):
    """Enabling dot.notation access to dictionary attributes and dynamic code assist in jupyter
    source: https://github.com/autogluon/autogluon/blob/master/eda/src/autogluon/eda/state.py
    """

    _getattr__ = dict.get
    __delattr__ = dict.__delitem__

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

        for arg in args:
            if isinstance(arg, dict):
                for k, v in arg.items():
                    self[k] = v

        for k, v in kwargs.items():
            self[k] = v

    def __getattr__(self, item) -> Any:
        # needed for mypy checks
        # try to get the default attribute from Dict
        return self._getattr__(item) if item in self else getattr(super, item, None)

    def __setattr__(self, name: str, value) -> None:
        try:
            getattr(super, name)  # check if name already exists in the default attribute of the Dict
        except AttributeError:
            if isinstance(value, dict):
                value = DotDict(value)
            self[name] = value
        else:
            raise KeyError(f'Name {name} already exists in the default attribute of the dict, use another name.')

    def __setitem__(self, key, value) -> None:
        if isinstance(value, dict):
            value = DotDict(value)
        super().__setitem__(key, value)

    @property
    def __dict__(self):
        return self

    @classmethod
    def from_yaml(cls, file_path: str) -> 'DotDict':
        with open(file_path, 'r') as file:
            yaml_data = yaml.safe_load(file)
        return cls(yaml_data)
