import logging.config

from pyspark.sql.functions import *
logging.config.fileConfig('Properties/configuration/logging.config')

logger = logging.getLogger('Create_spark')


def get_current_date(spark):
    try:
        logger.warning("hiiii")
        logger.warning('started the get_current_date method...')
        output = spark.sql("""select current_date""")
        logger.warning("validating spark object with current date-" + str(output.collect()))

    except Exception as e:
        logger.error('An error occured in get_current_date', str(e))

        raise

    else:
        logging.warning('Validation done , go frwd...')


def df_count(df,dfName):
    try:
        logger.warning(' no of records in {}'.format(dfName))

        df_c = df.count()
    except Exception as e:
        raise

    else:
        logger.warning('No of records present in   {} are  ::{}'.format(df,df_c))

    return df_c





