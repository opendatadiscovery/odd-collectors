from abc import ABC, abstractmethod


class BaseSecretsBackend(ABC):
    """
    Abstract base class to retrieve collector settings and plugins information
    from secrets backend.
    """

    @abstractmethod
    def get_collector_settings(self):
        raise NotImplementedError

    @abstractmethod
    def get_plugins(self):
        raise NotImplementedError
