from pyspark.sql.functions import rand
from sklearn.base import BaseEstimator, TransformerMixin
import numpy as np
import pandas as pd

from DataIO import load_y, load_specific_X


def create_X(logDf, fatpDf):
    """Create X matrix for predictive modeling

    Notes:

    Args:
        logDf: the logDf 
        fatpDf: 

    Returns:   A test item DataFrame ['serial_number','items']

    self.meta: A metadata DataFrame ['serial_number', 'version', 'line', 'machine', 'slot', 'hour']

    """
    # logging.info('Creating the matrix X...')
    logPassDf = logDf[(logDf['test_result'] == 'PASS') | (logDf['test_result'] == 'Pass')]
    logFatpDf = (logPassDf.join(fatpDf, logPassDf.serial_number == fatpDf.mlb_sn, 'inner')
                          .select('serial_number', 'items', 'version', 'line', 'machine', 'slot', 'hour'))

    ### unique SNs being moved
    logFatpDfDist = logFatpDf.dropDuplicates(['serial_number'])


    items = logFatpDfDist.select('serial_number', 'items')
    # meta = logFatpDfDist.select('serial_number','version', 'line', 'machine', 'slot', 'hour')
    # count = logFatpDfDist.count()

    return items


def combine_matrix(X, y, top = 4):
    """Create the data matrix for predictive modeling

    Notes: The default top n number is 5

    Args:
        X(SparkSQL DataFrame):
        y(SparkSQL DataFrame):

    Return:
        matrixAll(SparkSQL DataFrame):

    """
    # logging.info('Creating the big matrix X:y...')
    # y = hc.createDataFrame(y)
    ### Change y's column name 'serial_number' to 'SN'
    y = y.withColumnRenamed('serial_number', 'SN')
    ### Join X and y on serial_number, SN
    ### Add a new column 'y' specify return (1) or pass (0)
    matrixAll = (X.join(y, X.serial_number == y.SN, how = 'left_outer')
                  .withColumn('y', y['SN'].isNotNull().cast('int')))

    # matrixAll.cache()
    ### Drop row that has null values
    matrixAllDropNa = matrixAll.dropna(how = 'any')
    
    # matrixAllDropNa.cache()
    print 'to pandas()'
    symptomLocationPdf = matrixAllDropNa[['check_in_code', 'fail_location']].toPandas()
    print 'complete toPandas()
    # locationPdf = matrixAllDropNa[['fail_location']].toPandas()
    #return symptomPdf
    #return matrixAllDropNa, matrixAll
    
    codeSeries = symptomLocationPdf['check_in_code'].value_counts()
    #print codeSeries
    locationSeries = symptomLocationPdf['fail_location'].value_counts()
    ### Top N = 5 symptoms
    codeDict = {}
    locationDict = {}
    for i in range(top):
        # top n check in codes
        code = codeSeries.index[i]
        #codeLabel = 'code_{}'.format(i)
        codeLabel = '{}'.format(code)
        codeDict[code] = codeSeries[i]
        print 'top {} symptom: {}, count: {}'.format(i+1, code, codeSeries[i])
        matrixAll = (matrixAll.withColumn(codeLabel, (matrixAll['check_in_code'].like('%{}'.format(code))).cast('int'))
                              .fillna({codeLabel: 0}))

        # top n fail locations
        location = locationSeries.index[i]
        #locationLabel = 'location_{}'.format(i)
        locationLabel = '{}'.format(location)
        locationDict[location] = locationSeries[i]
        #print location
        print 'top {} fail location: {}, count: {}'.format(i+1, location, locationSeries[i])
        matrixAll = (matrixAll.withColumn(locationLabel, (matrixAll['fail_location'].like('%{}'.format(location))).cast('int'))
                              .fillna({locationLabel: 0}))

    # add a random integer column from 1 to 100 for later on sampling of training samples
    matrixAllRandDf = matrixAll.withColumn('random', rand())

    # transform the float random number to integer between 1 to 100
    matrixAllIntDf = matrixAllRandDf.withColumn('randInt', (matrixAllRandDf.random * 100).cast('int'))
    
    # cache the whole matrix table
    matrixAllIntDf.cache()
    
    return matrixAllIntDf

def get_y(cachedMatrix, **target):
    """Get y for building model
    
    Notes:  
            1. Make sure cache the matrix first before calling get_y function
    
            2. 
            Default value on code and location are none, if not specified, y of all return board will be set to 1
            before calling this function, make sure that the matrix has been cached.

    Argus:
        matrix:The complete DataFrame created from create_matrix()
        Dictionary:
                    target {key: value}
                            key:
                                code: the check in code treated as target varialbe y = 1
                                location: the replaced location treated as target variable y = 1
                                
    Return (numpy array): y
    
    """
    
    if 'code' in target:
        print target['code']
        #self.matrix[self.matrix]
        targety = np.array(cachedMatrix[[target['code']]].collect())
        targety = np.squeeze(targety)
    elif 'location' in target:
        print target['location']
        targety = np.array(cachedMatrix[[target['location']]].collect())
        targety = np.squeeze(targety)
    else:
        ### no target variable was set, y of all return board are set to y = 1
        print 'no target specified'
        targety = np.array(cachedMatrix.map(lambda x: x.y).collect())

    return targety



def create_matrix(date, hiveContext, sparkContext, model='N71', station='FCT'):
    '''create matrix according to specified model and test station

    Notes: the dates is a python list

    Args:
        dates: a list containing a dates
        hiveContext:
        sparkContext:

    Returns:

        a spark DataFrame

    '''
    # load logDf, fatpDf
    print 'loading logDf, fatpDf'
    logDf, fatpDf = load_specific_X(date, hiveContext, model=model, station=station)
    # laod y
    print 'loading fatpDF'
    rpcDf = load_y(sc=sparkContext)
    # combine X
    print 'create X Df'
    X = create_X(logDf, fatpDf)
    # create matrix
    print 'create matrix'
    matrix = combine_matrix(X, rpcDf)
    
    # cache the matrix
    # print 'cache the matrix'
    # matrix.cache()

    return matrix




def random_pdf(matrix, randIntList, returnboard=True):
    '''


    '''

    matrixPass = matrix[matrix['y'] == 0]
    matrixReturn = matrix[matrix['y'] == 1]
    matrixPassSample = matrixPass[matrixPass['randInt'].isin(randIntList)]
    if returnboard == True:
        matrixSample = matrixReturn.unionAll(matrixPassSample)
    else:
        matrixSample = matrixPassSample

    y = get_y(matrixSample)

    pdf = pd.DataFrame(matrixSample.map(lambda x: x.items).collect())



    return pdf, y






