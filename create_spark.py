from pyspark.sql import SparkSession
import logging.config


logger = logging.getLogger('Create_spark')

"""
if code is running in dev env call cluster master 
else run prod  use yarn  cluster dev 
"""

def get_spark_object(envn,appName):

    try:
        logger.info("get_spark method started ")
        if envn =='DEV':
            master = 'local'

        else:
            master ='Yarn'

        logger.info(' master is {}'.format(master))


        spark = SparkSession.builder.master(master).appName(appName).config("spark.pyspark.python", "python3.10").getOrCreate()


    except Exception as exp:
        logger.error('An error occured in get_spark_object====='.format(str(exp)))
        raise

    else:
        logger.info('Spark object created ')

    return spark
