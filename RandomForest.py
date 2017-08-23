from sklearn import ensemble
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import SelectFromModel
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from random import randint
from sklearn.metrics import confusion_matrix
from pprint import pprint
import numpy as np

import Feature_Generation
import Sampling


def fit_forest(x, y):

    #rf_model = Pipeline([
    #    ('feature selection', SelectFromModel(LinearSVC())),
    #     ('classification', ensemble.RandomForestClassifier(n_estimators=100, max_features=None))
    #])

    rf_model = ensemble.RandomForestClassifier(n_estimators=50, max_features=None)
    #rf_model = LogisticRegression(random_state=None, max_iter=5000)
    #rf_model = LinearSVC()
    #rf_model = GaussianNB()
    #rf_model = KNeighborsClassifier(n_neighbors=2)
    rf_model.fit(x, y)
    #importances = rf_model.feature_importances_
    #print("\nImportances:")
    #for a,b in zip(selected_features, importances):
    #    print(a,b)
    #print("\n")

    #print(rf_model.oob_score_)
    #print(rf_model.oob_decision_function_)

    return rf_model

def get_importances(model, features):
    importances = model.feature_importances_
    res = {}
    for i, f in enumerate(features):
        res[f] = importances[i]

    return res




def validate_model_new(model, df, selected_features):
    repetitions = 10

    accuracies = []
    drunk_accuracies = []
    conf_mats = []

    for i in range(repetitions):
        features, targets = Sampling.get_validation_inputs(df, selected_features, 12)

        predicted_targets = model.predict(features)
        conf_mat = confusion_matrix(targets, predicted_targets)
        accuracy = model.score(features, targets)
        drunk_accuracy = (conf_mat[-1][-1]) / (np.array(conf_mat[-1]).sum())

        accuracies.append(accuracy)
        drunk_accuracies.append(drunk_accuracy)
        conf_mats.append(conf_mat)
        #print(conf_mat, accuracy)

    mean_accuracy = np.array(accuracies).mean()
    mean_drunk_accuracy = np.array(drunk_accuracies).mean()

    average_conf_mat = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]
    #average_conf_mat = [[0, 0], [0, 0]]
    for m in conf_mats:
        for row_index, row in enumerate(average_conf_mat):
            for col_index, value in enumerate(row):
                average_conf_mat[row_index][col_index] += m[row_index][col_index]
    average_conf_mat = np.array(average_conf_mat)
    average_conf_mat = np.multiply(0.1, average_conf_mat)

    #print(average_conf_mat)

    print("mean accuracy = " + str(mean_accuracy))
    print("mean drunk accuracy = " + str(mean_drunk_accuracy))

    return (mean_accuracy, mean_drunk_accuracy, average_conf_mat)



