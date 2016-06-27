from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer, IDF
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import Row, SQLContext


def filter_col(matrix, colInfo, inputCol, outputCol):
    '''
    '''
    matrix['outputCol'] = matrix['inputCol'].map()




def parallel_learn_logistic(X, y, colInfo):
    '''use spark ML learn logistic model

    Notes:

    Args:

    Return:



    '''

    # preprocess
    # 1. filter columns according to colInfo


    # 2. transform


    # 3.

