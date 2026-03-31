from abc import ABC, abstractmethod
from typing import Optional

import numpy as np


class BaseDetector(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def update(self,
               current: np.ndarray,
               reference: np.ndarray) -> Optional[dict]:
        """
        Compare current batch embeddings against the reference window.
        Returns an alert dict if drift is detected, None otherwise.
        """
        pass