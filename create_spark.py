from pyspark.sql import SparkSession
import logging.config

logger = logging.getLogger('Create_spark')

def get_spark_object(envn, appName):
    try:
        logging.info('get_spark_object method started')

        if envn == 'DEV':
            master = 'local'

        else:
            master = 'Yarn'

        logger.info('master is {}'.format(master))

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

    except Exception as exp:
        logger.error('An error occured in the get_spark_object==== ',str(exp))
        raise

    else:
        logger.info('Spark object created........')
    return spark