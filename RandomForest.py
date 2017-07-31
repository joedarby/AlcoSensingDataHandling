from sklearn import ensemble
from random import randint
from sklearn.metrics import confusion_matrix
from pprint import pprint
import numpy as np

import Gait_Analysis



def fit_forest(x, y, selected_features):
    rf_model = ensemble.RandomForestClassifier()
    rf_model.fit(x, y)
    importances = rf_model.feature_importances_
    print("\nImportances:")
    for a,b in zip(selected_features, importances):
        print(a,b)
    print("\n")

    #print(rf_model.oob_score_)
    #print(rf_model.oob_decision_function_)

    return rf_model


def validate_model(model, validation_data, selected_features):
    number_of_samples = 30
    output = []
    accuracies = []

    repetitions = 10

    for i in range(repetitions):
        drunk_periods = []
        sober_periods = []
        data_size = len(validation_data)
        while len(drunk_periods) <= (number_of_samples / 2):
            num = randint(0, data_size - 1)
            selection = validation_data[num]
            if selection[0]["survey"] is not None:
                if selection[0]["survey"]["feeling"] > 1:
                    drunk_periods.append(selection)
        while len(sober_periods) <= (number_of_samples / 2):
            num = randint(0, data_size - 1)
            selection = validation_data[num]
            if selection[0]["survey"] is not None:
                if selection[0]["survey"]["feeling"] <= 1:
                    sober_periods.append(selection)

        selected_periods = drunk_periods + sober_periods

        validation_features, validation_targets = Gait_Analysis.generate_model_inputs(selected_periods, selected_features)
        predicted_targets = model.predict(validation_features)
        conf_mat = confusion_matrix(validation_targets, predicted_targets)
        accuracy = model.score(validation_features, validation_targets)
        result = [conf_mat, accuracy]

        output.append(result)
        accuracies.append(accuracy)

    mean_accuracy = np.array(accuracies).mean()

    #pprint(output)
    print("mean accuracy = " + str(mean_accuracy))

    return mean_accuracy


