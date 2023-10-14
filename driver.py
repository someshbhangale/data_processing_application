import os

from time import perf_counter

import get_env_variables as gav
from create_spark import get_spark_object
from validate import get_current_date, print_schema, check_for_nulls
from ingest import load_files, display_df, df_count
from data_processing import *
from data_transformation import *
from extraction import *
import logging
import logging.config
import sys

logging.config.fileConfig('Properties/Configuration/logging.config')


def main():
    global file_format, file_dir, header, inferSchema
    try:
        start_time = perf_counter()

        logging.info('i am in the main method...')
        # print("i am in the main")
        # print(gav.header)
        # print(gav.src_olap)
        logging.info('calling spark object')
        spark = get_spark_object(gav.envn, gav.appName)

        logging.info('Validating spark object.............')
        get_current_date(spark)

        # print(os.listdir(gav.src_olap))
        # Below Loop we are using if Folder contain more than one file, then we will iterate it

        for file in os.listdir(gav.src_olap):
            print("File is " + file)

            file_dir = gav.src_olap + '\\' + file
            # print(file_dir)

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

        logging.info('reading file which is of > {} '.format(file_format))

        df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)
        logging.info("displaying file")
        display_df(df_city, 'df_city')

        logging.info("validating the dataframe....")
        df_count(df_city, 'df_city')

        logging.info('checking for the files in the Fact/oltp......')

        for files in os.listdir(gav.src_oltp):
            print("Src Files::" + files)

            file_dir = gav.src_oltp + '\\' + files
            print(file_dir)

            if files.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

            elif files.endswith('.csv'):
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
        logging.info('reading file which is of > {}'.format(file_format))

        df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header,
                             inferSchema=inferSchema)

        logging.info('displaying the df_fact dataframe')
        display_df(df_fact, 'df_fact')

        # validate::
        df_count(df_fact, 'df_fact')

        logging.info("implementing data_processing methods.....")

        df_city_sel, df_presc_sel = data_clean(df_city, df_fact)

        display_df(df_city_sel, 'df_city')
        display_df(df_presc_sel, 'df_fact')

        logging.info("validating schema for the dataframes.......")
        print_schema(df_city_sel, 'df_city_sel')
        print_schema(df_presc_sel, 'df_presc_sel')

        display_df(df_presc_sel, 'df_fact')

        logging.info('checking for null values in the dataframes....after processing ')

        check_df = check_for_nulls(df_presc_sel, 'df_fact')
        display_df(check_df, 'df_fact')

        logging.info('data_transformation executing.....')

        df_report_1 = data_report1(df_city_sel, df_presc_sel)
        logging.info('displaying the df_report1')
        display_df(df_report_1, 'data_report1')

        logging.info('displaying data_report2 method....')

        df_report_2 = data_report2(df_presc_sel)
        logging.info('displaying the df_report2')
        display_df(df_report_2, 'data_report2')

        # Writing a File to Specified Directory
        logging.info('extracting files to Output....')
        city_path = gav.city_path

        # extract_files(df_report_1, 'orc', city_path, 1, False, 'snappy')
        #df_report_2.write.mode('overwrite').csv(city_path)

        logging.info('extracting files to Output completed....')

        # Print the total processing time taken
        end_time = perf_counter()
        print(f"Total Process Time {end_time - start_time: 0.2f} seconds()")

    except Exception as exp:
        logging.error(" An error occured when calling main() please check the trace=== ", str(exp))
        sys.exit(1)


if __name__ == '__main__':
    main()
    logging.info('Application done')
