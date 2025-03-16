import abc
import json
import logging
import os
from typing import Any, Dict
from uuid import UUID

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseStorage(abc.ABC):
    """Абстрактное хранилище состояния.

    Позволяет сохранять и получать состояние.
    Способ хранения состояния может варьироваться в зависимости
    от итоговой реализации. Например, можно хранить информацию
    в базе данных или в распределённом файловом хранилище.
    """

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Сохранить состояние в хранилище."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Получить состояние из хранилища."""


class JsonFileStorage(BaseStorage):
    """Implementation of storage using a local file. Format: JSON"""

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Save the state to the storage, converting UUID to string."""

        # Convert UUIDs to string for JSON serialization
        def convert_uuid(obj):
            if isinstance(obj, UUID):
                return str(obj)
            raise logger.error(
                f"Object of type {obj.__class__.__name__} is not serializable"
            )

        try:
            with open(self.file_path, "w") as f:
                json.dump(state, f, ensure_ascii=False, indent=4, default=convert_uuid)
        except IOError as e:
            logger.error(f"Error writing to file {self.file_path}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while saving the state: {e}")

    def retrieve_state(self) -> Dict[str, Any]:
        """Retrieve the state from the storage, converting strings back to UUID."""

        # Convert string back to UUID if possible
        def convert_uuid_from_string(obj):
            if isinstance(obj, str):
                try:
                    return UUID(obj)
                except ValueError:
                    pass
            return obj

        if not os.path.exists(self.file_path):
            logger.info(
                f"No state file found at {self.file_path}. Returning empty state."
            )
            return {}

        try:
            with open(self.file_path, "r") as f:
                state = json.load(f)
                return {
                    key: convert_uuid_from_string(value) for key, value in state.items()
                }
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from file {self.file_path}: {e}")
            return {}
        except IOError as e:
            logger.error(f"Error reading from file {self.file_path}: {e}")
            return {}
        except Exception as e:
            logger.error(f"An unexpected error occurred while reading the state: {e}")
            return {}


class State:
    """Класс для работы с состояниями."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа."""
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу."""
        return self.state.get(key, None)
