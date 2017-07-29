from sklearn import ensemble



def fit_forest(x, y):
    rf_model = ensemble.RandomForestClassifier()
    rf_model.fit(x, y)
    print(rf_model.feature_importances_)
    #print(rf_model.oob_score_)
    #print(rf_model.oob_decision_function_)

    return rf_model