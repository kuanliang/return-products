
def isfloat(string):
    try:
        float(string)
        return True
    except:
        return False


def cache_matrix(matrix, *columns):
    
    matrixCached = matrix[list(columns)]
    matrixCached.cache()