import logging.config

logging.config.fileConfig('Properties/Configuration/logging.config')

logger = logging.getLogger('Ingest')


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.warning("load_files method started")

        if file_format == 'parquet':
            df = spark. \
                read. \
                format(file_format). \
                load(file_dir)
        elif file_format == 'csv':

            df = spark. \
                read. \
                format(file_format). \
                options(header=header). \
                options(inferSchema=inferSchema). \
                load(file_dir)

    except Exception as e:
        logger.error("An error occurred while dealing with load_file ====" + str(e))
        raise
    else:
        logger.warning("Load_files func done, go fwd..")

    return df


def display_df(df, dfName):
    df_show = df.show()

    return df_show


def df_count(df, dfName):
    try:
        logger.warning('here to count the records in the {}'.format(dfName))

        df_c = df.count()

    except Exception as exp:
        raise
    else:
        logger.warning("Number of records present in the {} are :: {}".format(df, df_c))

    return df_c
