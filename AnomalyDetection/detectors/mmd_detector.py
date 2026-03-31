import logging
import time
from typing import Optional

import numpy as np

from detectors.base_detector import BaseDetector
import config

log = logging.getLogger("detector.mmd")


class MMDDetector(BaseDetector):

    def __init__(self,
                 threshold: float = config.MMD_THRESHOLD,
                 sample_size: int = config.MMD_SAMPLE_SIZE):
        self.threshold = threshold
        self.sample_size = sample_size

    @property
    def name(self) -> str:
        return "mmd"

    def update(self, current: np.ndarray, reference: np.ndarray) -> Optional[dict]:
        try:
            if len(current) < 10 or len(reference) < 10:
                return None

            x = self._sample(current, self.sample_size)
            y = self._sample(reference, self.sample_size)

            score = self._mmd_rbf(x, y)
            log.debug("MMD score: %.4f (threshold: %.4f)", score, self.threshold)

            if score > self.threshold:
                log.info("MMD drift detected! score=%.4f", score)
                return {
                    "detector":    self.name,
                    "score":       float(score),
                    "threshold":   self.threshold,
                    "detected_at": int(time.time() * 1000),
                }
        except Exception as e:
            log.warning("MMD error: %s", e)
        return None

    def _mmd_rbf(self, x: np.ndarray, y: np.ndarray) -> float:
        # median trick for bandwidth selection
        xy = np.vstack([x, y])
        dists = np.sum((xy[:, None] - xy[None, :]) ** 2, axis=-1)
        sigma2 = np.median(dists[dists > 0])
        if sigma2 == 0:
            return 0.0

        def rbf(a, b):
            d = np.sum((a[:, None] - b[None, :]) ** 2, axis=-1)
            return np.exp(-d / (2 * sigma2))

        kxx = rbf(x, x)
        kyy = rbf(y, y)
        kxy = rbf(x, y)

        np.fill_diagonal(kxx, 0)
        np.fill_diagonal(kyy, 0)
        n, m = len(x), len(y)

        mmd2 = (kxx.sum() / (n * (n - 1))
                + kyy.sum() / (m * (m - 1))
                - 2 * kxy.mean())
        return float(max(mmd2, 0.0)) ** 0.5

    def _sample(self, arr: np.ndarray, n: int) -> np.ndarray:
        if len(arr) <= n:
            return arr
        idx = np.random.choice(len(arr), n, replace=False)
        return arr[idx]