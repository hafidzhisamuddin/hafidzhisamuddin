from sklearn import metrics as skmetrics
from typing import List
import numpy as np


class RegressionMetrics:
    def __init__(self):
        """Base Class which takes the actual, predicted values & metric type and calculates the metric value
        """
        self.metrics = {
            "mae": self._mae,
            "mse": self._mse,
            "rmse": self._rmse,
            "msle": self._msle,
            "r2": self._r2,
            "mape": self._mape
        }

    def __call__(self, metric: str, y_true: List, y_pred: List) -> str:
        """Returns the metric value selected

        Args:
            metric (str): Metric that we are interested in
            y_true (List): List of Actual values
            y_pred (List): List of Predicted Values

        Raises:
            Exception: if the metric passed is not in the defined metrics

        Returns:
            str: calculated metric value
        """
        if metric not in self.metrics:
            raise Exception("Metric Not Implemented")
        if metric == "mae":
            return self._mae(y_true, y_pred)
        if metric == "mse":
            return self._mse(y_true, y_pred)
        if metric == "rmse":
            return self._rmse(y_true, y_pred)
        if metric == "msle":
            return self._msle(y_true, y_pred)
        if metric == "r2":
            return self._r2(y_true, y_pred)
        if metric == "mape":
            return self._mape(y_true, y_pred)

    @staticmethod
    def _mae(y_true, y_pred):
        '''Calculates Mean Absolute Error'''
        return skmetrics.mean_absolute_error(y_true, y_pred)

    @staticmethod
    def _mse(y_true, y_pred):
        '''Calculates Mean Squared Error'''
        return skmetrics.mean_squared_error(y_true, y_pred)

    @staticmethod
    def _rmse(y_true, y_pred):
        '''Calculates Root Mean Squared Error'''
        return np.sqrt(skmetrics.mean_squared_error(y_true, y_pred))

    @staticmethod
    def _msle(y_true, y_pred):
        '''Calculates Root Mean Squared Log Error'''
        return skmetrics.mean_squared_log_error(y_true, y_pred)

    @staticmethod
    def _r2(y_true, y_pred):
        '''Calculates R-Square'''
        return skmetrics.r2_score(y_true, y_pred)

    @staticmethod
    def _mape(y_true, y_pred):
        '''Calculates Mean Absolute Percentage Error'''
        # initialize error at 0
        error = 0
        for yt, yp in zip(y_true, y_pred):
            # calculate percentage error
            # and add to error
            error += np.abs(yt-yp) / yt
            # return mean percentage error
            return error / len(y_true)
