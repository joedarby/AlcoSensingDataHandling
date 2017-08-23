from sklearn.linear_model import LogisticRegression
from sklearn.linear_model import LinearRegression
from sklearn import preprocessing


def build_logistic_model(features, targets):
    #features_scaled = preprocessing.scale(features)
    #scaler = preprocessing.StandardScaler().fit(features)

    model = LogisticRegression(random_state=None, max_iter=5000)
    model.fit(features, targets)
    print(model.classes_)
    print(model)

    return model


