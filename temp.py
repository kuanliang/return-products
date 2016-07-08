import pandas as pd
import numpy as np

def get_y(matrix, **target):
    """Get y for building model
    Notes:  Default value on code and location are none, if not specified, y of all return board will be set to 1

    Argus:
        matrix:The complete DataFrame created from create_matrix()
        Dictionary:
                    target {key: value}
                            key:
                                code: the check in code treated as target varialbe y = 1
                                location: the replaced location treated as target variable y = 1
    """

    if 'code' in target:
        print target['code']
        #self.matrix[self.matrix]
        targety = np.array(matrix[[target['code']]].collect())
        targety = np.squeeze(targety)
    elif 'location' in target:
        print target['location']
        targety = np.array(matrix[[target['location']]].collect())
        targety = np.squeeze(targety)
    else:
        ### no target variable was set, y of all return board are set to y = 1
        print 'no target specified'
        targety = np.array(matrix.map(lambda x: x.y).collect())

    return targety

def random_pdfy(matrix, randIntList, returnboard=True):
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
