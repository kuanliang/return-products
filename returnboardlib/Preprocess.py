import pandas as pd
from pyspark import HiveContext
from sklearn.base import BaseEstimator, TransformerMixin

def analyze_column(sc, X):
    """analyze column by


    Note:

    Args(object):    Spark DataFrame created MatrixCreator

    Return:
        colInfo(dictionary):
        {
            'version': 'test_version'
            'preprocess':
                {
                    'all': [col1, col2, ... , colN],
                    'singleton': [col1, col2, ... , colN],
                    'string': [col1, col2, ... , colN],
                    'final': [col1, col2, ... , colN]
                }
        }

    """
    hc = HiveContext(sc)
    colInfo = {}
    preprocess = {}

    # 1. Create X matrix from matrixCreator
    # 2. Sampling fro this X matrix
    # 3. trasform the X matrix to dictionary
    # 4.
    sampleX = (X.sample(withReplacement=False, fraction=0.005, seed=42)
                .map(lambda x: x.items))

    df = pd.DataFrame(sampleX.collect())
    # df = df.toPandas()
    preprocess['all'] = list(df.columns)
    ori_num = len(df.columns)
    dfNu = df.apply(pd.to_numeric, errors='coerce')
    dfNuRM = dfNu.dropna(axis=1, how='all')
    nu_num = len(dfNuRM.columns)
    diff = list(set(df.columns) - set(dfNuRM.columns))

    preprocess['string'] = diff

    remove_count = 0
    colSet = set(dfNuRM.columns)
    for col in dfNuRM.columns:
        if len(dfNuRM[col].value_counts()) == 1:
            del dfNuRM[col]
            remove_count += 1
    print 'There are {} columns being removed'.format(remove_count)

    diff = list(colSet - set(dfNuRM.columns))

    preprocess['singleton'] = diff
    preprocess['final'] = list(dfNuRM.columns)

    colInfo['preprocess'] = preprocess

    try:
        listLen = 0
        listList = []
        for key in colInfo['preprocess'].keys():
            if key != 'all':
                listLen += len(colInfo['preprocess'][key])
                listList += list(colInfo['preprocess'][key])

        if((listLen == len(colInfo['preprocess']['all'])) & (set(list(colInfo['preprocess']['all'])) == set(listList))):
            print 'the number is correct'
    except:
        print 'the number is incorrect'

    return colInfo

class InitialProcessor(BaseEstimator, TransformerMixin):
    """Initial preprocess the dataframe
             1. Transform cells to numeric: errors = 'coerce', invalid parsing will be set as NaN
             2. Drop NaN: how = 'all', axis = 1, if all values are NaN, drop that label
             3. Drop singleton columns:
             4. input missing value with column median

    """

    def __init__(self, colInfo):
        self.colInfo = colInfo

    def fit(self, x, y=None):
        return self

    def transform(self, df):
        """preprocess the data by removing columns thosed defined in colInfo (string, singleton)

        Notes:

        Args:
            df(Pandas DataFrame):

            colInfo(dictionary):

        Return:
            df(Pandas DataFrame): A dataframe after (string, singleton) column removal

        """
        colInfo = self.colInfo
        ### Step 1. select columns those defined in colInfo
        dfProcessed = df[colInfo['preprocess']['final']]

        ### Step 2. Transform cells to numeric values, invalid parsing will be set to NaN
        dfNu = dfProcessed.apply(pd.to_numeric, errors = 'coerce')
        ### Step 3. Drop columns if all values in that column are all NaN
        dfNu.dropna(axis = 1, how = 'all', inplace = True)
        ### Step 3. Drop singleton columns
        #remove_count = 0
        #for col in dfNu.columns:
        #    if len(dfNu[col].value_counts()) == 1:
        #        del dfNu[col]
        #        remove_count += 1
        #print 'There are {} columns being removed'.format(remove_count)
        #print (len(dfNu.columns))

        ### Step 4. input missing value with column median
        #dfNu.fillna(method = 'median')

        return dfNu
