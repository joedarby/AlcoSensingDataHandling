from sklearn import ensemble
from random import randint
from sklearn.metrics import confusion_matrix
from pprint import pprint
import numpy as np

import Feature_Generation
import Sampling


def fit_forest(x, y, selected_features):
    rf_model = ensemble.RandomForestClassifier()
    rf_model.fit(x, y)
    importances = rf_model.feature_importances_
    #print("\nImportances:")
    #for a,b in zip(selected_features, importances):
    #    print(a,b)
    #print("\n")

    #print(rf_model.oob_score_)
    #print(rf_model.oob_decision_function_)

    return rf_model


def validate_model_new(model, df, selected_features):
    repetitions = 20

    output = []
    accuracies = []

    for i in range(repetitions):
        features, targets = Sampling.get_validation_inputs(df, selected_features)

        predicted_targets = model.predict(features)
        #conf_mat = confusion_matrix(targets, predicted_targets)
        accuracy = model.score(features, targets)
        #result = [conf_mat, accuracy]

        #output.append(result)
        accuracies.append(accuracy)

    mean_accuracy = np.array(accuracies).mean()

    #for o in output:
    #    print(o[0], o[1])
    print("mean accuracy = " + str(mean_accuracy))

    return mean_accuracy



