from Preprocess import InitialProcessor
import pandas as pd
import numpy as np
# SKLearn Libraries
from sklearn import linear_model, decomposition
from sklearn.svm import SVC
from sklearn import cross_validation
from sklearn.pipeline import FeatureUnion
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import Imputer, RobustScaler
# RobustScaler
from sklearn.metrics import classification_report
from sklearn import grid_search

# outside package
from Transform import get_y

import sys


def learn_logistic(X, y, colInfo):
    """

    """
    print 'start logistic MODELING...'
    #print 'X: {}'.format(type(X))
    #preprocessing = FeatureUnion([("initial", InitialProcessor())])
    IP = InitialProcessor(colInfo=colInfo)

    # print 'X value: {}'.format(type(X))
    X = pd.DataFrame(X, columns=colInfo['preprocess']['all'])
    X = IP.transform(X)
    pipe = Pipeline([
            #('union', InitialProcessor(colInfo=colInfo)),
            ('impute', Imputer(missing_values = 'NaN', strategy = 'median', axis = 1)),
            ('scaler', RobustScaler()),
            #('clf', linear_model.SGDClassifier(loss = 'log', penalty='l1', class_weight='balanced', n_iter=1))
            ('clf', linear_model.LogisticRegression(class_weight='balanced'))
        ])

    param_grid = {
        'clf__penalty': ('l1'),
        'clf__tol': (1, 1e-1, 1e-2, 1e-3, 1e-4),
        'clf__C': (10, 5, 1, 0.1, 0.01, 0.001, 0.0001)
    }
    X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size = 0.3, random_state=0)
    gs_cv = grid_search.GridSearchCV(pipe, param_grid, scoring='precision')
    gs_cv.fit(X_train, y_train)
    predict_test = gs_cv.predict(X_test)
    predict_train = gs_cv.predict(X_train)
    prediction_report_test = classification_report(y_test, predict_test)
    prediction_report_train = classification_report(y_train, predict_train)
    # self.predicion_report_test = prediction_report_test
    # self.prediction_report_train = prediction_report_train
    return gs_cv, prediction_report_test, prediction_report_train


def learn_SVM(X, y, colInfo):
    """

    """
    print 'start SVM MODELING...'
    #print 'X: {}'.format(type(X))
    #preprocessing = FeatureUnion([("initial", InitialProcessor())])
    IP = InitialProcessor(colInfo=colInfo)

    # print 'X value: {}'.format(type(X))
    X = pd.DataFrame(X, columns=colInfo['preprocess']['all'])
    X = IP.transform(X)
    # pca = decomposition.PCA()
    pipe = Pipeline([
            #('union', InitialProcessor(colInfo=colInfo)),
            ('impute', Imputer(missing_values = 'NaN', strategy = 'median', axis = 1)),
            ('scaler', RobustScaler()),
            #('clf', linear_model.SGDClassifier(loss = 'log', penalty='l1', class_weight='balanced', n_iter=1))
            # ('pca', pca)
            ('clf', SVC(kernel='rbf', class_weight='balanced', probability=True))
        ])

    param_grid = {
        'clf__C': (1, 1e2, 1e3, 1e4, 1e5),
        'clf__gamma': (1, 1e-1, 1e-2, 1e-3, 1e-4),
        'clf__tol': (1e-2, 1e-3, 1e-4, 1e-5)
    }
    X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size = 0.3, random_state=0)
    gs_cv = grid_search.GridSearchCV(pipe, param_grid, scoring='precision')
    gs_cv.fit(X_train, y_train)
    predict_test = gs_cv.predict(X_test)
    predict_train = gs_cv.predict(X_train)
    prediction_report_test = classification_report(y_test, predict_test)
    prediction_report_train = classification_report(y_train, predict_train)
    # self.predicion_report_test = prediction_report_test
    # self.prediction_report_train = prediction_report_train
    return gs_cv, prediction_report_test, prediction_report_train



def sampling_modeling(matrix, colInfo, classifier='SVM', parallel=False, iterative=False, **sampling):


    if sampling['samplingRatio'] not in [round(x,2) for x in np.arange(0.01, 1.01, 0.01)]:
        print 'please specify sampling ratio within list: {}'.format(np.linspace(0.01, 1, 100))
        print 'something wrong!!'
        sys.exit()
    # the X matrix
    else:
        samplingRatio = sampling['samplingRatio']

        # according to the specified sampling ratio, generate a list of random numbers that will be
        # used to filter training data later
        randIntList = range(1, int(samplingRatio * 100) + 1)

        matrixReturn = matrix[matrix['y'] == 1]
        matrixPass = matrix[matrix['y'] == 0]
        # matrixPassSample = matrixPass.sample(False, 0.01, 42)
        # rather than use dataframe sampling funciton, use the random integer gererated in matrix dataframe


        if iterative == False:
            matrixPassSample = matrixPass[matrixPass['randInt'].isin(randIntList)]
        else:
            matrixPassSample = matrixPass[matrixPass['randInt'] == samplingRatio]
        # unionAll dataframes
        matrixSample = matrixReturn.unionAll(matrixPassSample)
        matrixGet = matrixSample

        if parallel==False:
            # scikit-learn
            y = get_y(matrixGet)
            pdf = pd.DataFrame(matrixSample.map(lambda x: x.items).collect())

            if classifier == 'logistic':
                model, report_test, report_train = learn_logistic(X=pdf, y=y, colInfo=colInfo)
            elif classifier == 'SVM':
                model, report_test, report_train = learn_SVM(X=pdf, y=y, colInfo=colInfo)
        else:
            # sparkML
            model, report_test, report_train = parallel_learn_logistic(matrixGet, colInfo=colInfo)



    return model, report_test, report_train


def iterative_modeling(matrix, colInfo, classifier='logistic', parallel=False, **sampling):


    samplingRatio = sampling['samplingRatio']

    matrixReturn = matrix[matrix['y'] == 1]
    matrixPass = matrix[matrix['y'] == 0]
    # matrixPassSample = matrixPass.sample(False, 0.01, 42)
    # rather than use dataframe sampling funciton, use the random integer gererated in matrix dataframe
    matrixPassSample = matrixPass[matrixPass['randInt'] == ]
    # unionAll dataframes
    matrixSample = matrixReturn.unionAll(matrixPassSample)
    matrixGet = matrixSample

    if parallel==False:
        # scikit-learn
        y = get_y(matrixGet)
        pdf = pd.DataFrame(matrixSample.map(lambda x: x.items).collect())

        if classifier == 'logistic':
            model, report_test, report_train = learn_logistic(X=pdf, y=y, colInfo=colInfo)
        elif classifier == 'SVM':
            model, report_test, report_train = learn_SVM(X=pdf, y=y, colInfo=colInfo)
    else:
        # sparkML
        model, report_test, report_train = parallel_learn_logistic(matrixGet, colInfo=colInfo)



    return model, report_test, report_train






