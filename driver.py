import os
import get_env_variables as gav
from create_spark import get_spark_object
from validate import  get_current_date,df_count
import logging
from ingest import load,output_csv,display_df
import logging.config
import sys
from extraction import extract_files

logging.config.fileConfig('Properties/configuration/logging.config')


def main():
    global file_dir, header, inferSchema, file_format
    try:
        logging.info('MAIN METHOD STARTED')
        logging.info ('CALLING SPARK OBJECT')


        #To always sync master and servant python versions
        os.environ['PYSPARK_PYTHON'] = sys.executable
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

        spark = get_spark_object(gav.envn,gav.appName)

        logging.info(f'VALIDATING SPARK OBJECT.............')

        get_current_date(spark)



        #Made the file reading dynamic according to source
        for file in os.listdir(gav.src_oltp):
            file_dir = gav.src_oltp +'\\'+file

            if file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'N/A'
                inferSchema = 'N/A'


            elif file.endswith('.csv'):
                file_format = 'csv'
                header = gav.header

            logging.info('READING FILE OF FORMAT {}'.format(file_format))


        """
        
        AFTER READING THE FILE 
        DATA TRANSFORMATIONS BASED ON THE ASSESSMENT IS CARRIED OUT IN ingest.py CLASS 

        """
        df_final = load(spark = spark ,file_dir = file_dir ,file_format = file_format ,header = gav.header ,inferSchema = gav.inferSchema)


        """
        
        AFTER ALL THE TRANSFORMATIONS FINAL CSV FILE IS SAVED IN OUTPUT PATH -> OUTPUT/OUTPUT_CSV
        I HAVE ALSO SAVED IN PARQUET AND COMPRESSED IT 
        
        """

        # Saving the final output file in outpath ->output->csv_output
        output_csv(df_final, gav.output_path_csv)

        #Also saved the output file in parquet and compressed it
        extract_files(df_final, 'parquet', gav.output_path_parquet, 2, False, 'snappy')

        logging.info("PRInTING THE FINAL DF")
        display_df(df_final)



    except Exception as e:
        logging.error("an error occured when calling main() please check the trace=== ",str(e))
        sys.exit(1)



if __name__ == '__main__':
    main()
    logging.info('APPLICATION DONE')