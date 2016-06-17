from utility import isfloat
import numpy as np
import pandas as pd
from sklearn.metrics import classification_report
from sklearn.metrics import precision_recall_curve


def predict_test(matrix, model, colInfo, samplingRatio=0.01, **kwargv):
    """use the selected model to predict leftover records,

    Notes:



    Args:
        matrix: the matrix RDD
        modelsDict: the dictionary contains models
                    e.g. modelsDict['all']['model']
                         modelsDict['code']['code1']['model']

        **kwargv
        target['target'] = 'all'
        target['target'] = 'test'
        target['target'] = 'leftover'

    Return:

        a prediction report


    """
    # use the all model to do the prediction
    # model = modelsDict['all']['model']


    intList = range(1, int(samplingRatio * 100) + 1)
    intRevList = list(set(range(1,101)) - set(intList))

    colFinal = colInfo['preprocess']['final']

    if kwargv['target'] == 'all':
        # use the model to predict whole day records
        truePredictRdd = (matrix.map(lambda x: (x.y, x.items))
                                .map(lambda (a, b): (a, {col:b[col] if col in b.keys() else np.NAN for col in colFinal}))
                                .map(lambda (a, b): (a, {col:float(b[col]) if isfloat(b[col]) else np.NAN for col in colFinal}))
                                .map(lambda (a, b): (a, [b[col] for col in colInfo['preprocess']['final']]))
                                .map(lambda (a, b): (a, model.predict(b), model.predict_proba(b))))



    elif kwargv['target'] == 'validate':
        # use the model to predict original sampling recrod, for verification
        matrixReturn = matrix[matrix['y'] == 1]
        matrixPass = matrix[matrix['randInt'].isin(intList)]
        matrixSample = matrixReturn.unionAll(matrixPass)
        truePredictRdd = (matrixSample.map(lambda x: (x.y, x.items))
                                     .map(lambda (a, b): (a, {col:b[col] if col in b.keys() else np.NAN for col in colFinal}))
                                     .map(lambda (a, b): (a, {col:float(b[col]) if isfloat(b[col]) else np.NAN for col in colFinal}))
                                     .map(lambda (a, b): (a, [b[col] for col in colInfo['preprocess']['final']]))
                                     .map(lambda (a, b): (a, model.predict(b), model.predict_proba(b))))

    elif kwargv['target'] == 'leftover':
        matrixSample = matrix[matrix['randInt'].isin(intRevList)]
        truePredictRdd = (matrixSample.map(lambda x: (x.y, x.items))
                                     .map(lambda (a, b): (a, {col:b[col] if col in b.keys() else np.NAN for col in colFinal}))
                                     .map(lambda (a, b): (a, {col:float(b[col]) if isfloat(b[col]) else np.NAN for col in colFinal}))
                                     .map(lambda (a, b): (a, [b[col] for col in colInfo['preprocess']['final']]))
                                     .map(lambda (a, b): (a, model.predict(b), model.predict_proba(b))))



    true = truePredictRdd.map(lambda (a, b, c): a).collect()
    predict = truePredictRdd.map(lambda (a, b, c): b[0]).collect()
    predict_proba = truePredictRdd.map(lambda (a, b, c): c[0][1]).collect()

    prediction_report_all = classification_report(true, predict)
    precision, recall, thresholds = precision_recall_curve(true, predict_proba)

    return prediction_report_all, precision, recall, thresholds


