from datetime import date
date.today()
import datetime

from pyspark.sql import SQLContext


def load_specific_X(startDate, hiveContext, model='N71', station='FCT'):

    start_date = startDate
    end_date = date.today().strftime('%Y-%m-%d')
    start_time_log = start_date + '_00'
    end_time_log = start_date + '_23'
    parameters = {
        'model': model,
        'station': station,
        'start_time_log': start_time_log, 'end_time_log': end_time_log,
        'start_date_fatp': start_date, 'end_date_fatp': end_date}

    sqlLog = "select * from mlb_test_log_detail\
              where station = '{station}'\
              and model = '{model}'\
              and hour between '{start_time_log}' and '{end_time_log}'".format(**parameters)

    sqlFatp = "select * from fatp_r_wip_stage\
               where model = '{model}'\
               and day between '{start_date_fatp}' and '{end_date_fatp}'".format(**parameters)

    # global hc
    hiveContext.sql('use cpk')
    logDf = hiveContext.sql(sqlLog)
    fatpDf = hiveContext.sql(sqlFatp)

    # DF.distinct() will not be worked on map columns
    # use dropDuplicates instead
    logDfDist = logDf.dropDuplicates(['serial_number', 'test_start_time', 'hour'])
    fatpDfDist = fatpDf.dropDuplicates()


    return logDfDist, fatpDfDist


def load_y(sc):
    sqlContext = SQLContext(sc)
    rpcDf = sqlContext.read.format('jdbc').options(url='jdbc:sqlserver://10.206.49.41;datebase=rpc;user=sa;\
    password=1qaz2wsx3edc4rfv%TGB', dbtable='[rpc].[dbo].[rpc_day_andy]').load()
            # rpcPdf = rpcDf.toPandas()
    return rpcDf

