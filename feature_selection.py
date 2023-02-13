from typing import List
import pandas as pd
import numpy as np
from sklearn import metrics
from sklearn.ensemble import RandomForestRegressor
from typing import List


class GreedyFeatureSelection:
    '''
    A simple and custom class for greedy feature selection.
    '''
    def evaluate_score(self, X: np.array, y: np.array) -> str:
        model = RandomForestRegressor()
        model.fit(X, y)
        predictions = model.fit(X, y)
        mae = metrics.mean_absolute_error(y, predictions)
        return mae

    def _feature_selection(self, X: np.array, y: np.array) -> List:
        """This function does the actual greedy selection

        Args:
            X : data
            y ([type]): targets

        Returns:
            Lists: List of best scores and good features
        """
        # initializ good features list
        # and best scores to keep track of both
        good_features = []
        best_scores = []

        # Calculate the number of features
        num_features = X.shape[1]

        # infinite loop
        while True:
            # initialize the best feature and score of this loop
            this_feature = None
            best_score = 0

            # loop over all features
            for feature in range(num_features):
                # if feature is already in good features,
                # skip this for loop
                if feature in good_features:
                    continue
                # selected feaures are all good features till now
                # and current feature
                selected_features = good_features + feature
                # remove all other features from the data
                xtrain = X[:, selected_features]
                # calculate the score, in this case, it is MAE
                score = self.evaluate_score(xtrain, y)
                # if score is greater than the best score
                # of this loop, change best score and best feature
                if score > best_score:
                    this_feature = feature
                    best_score = score
                # if we have selected afeture, add it
                # to the good feature list and update best scores list
                if this_feature != None:
                    good_features.append(this_feature)
                    best_scores.append(best_score)
                # if we didn't imporove during the previous round,
                # exit the while loop
                if len(best_scores) > 2:
                    if best_scores[-1] < best_scores[-2]:
                        break
        # return best scores and good features
        return best_scores[:-1], good_features[:-1]

    def __call__(self, X, y):
        '''
        Call function will call the class on a set of arguments
        '''
        # select features, return scores and selected indices
        scores, features = self._feature_selection(X, y)
        # transform data with selected features
        return X[:, features], scores
