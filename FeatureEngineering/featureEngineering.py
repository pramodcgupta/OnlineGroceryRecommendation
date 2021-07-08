# =======================================================================================================
# # ---------------------------------------------------------------------------------------------------
# # Creation Date: 23-Jun-2021
# # Author: PramodKumar Gupta
# # Script: featureEngineering.py
# # Purpose: This script is created to perform feature engineering needed for recommendation project.
# # Version: 1.0
# # Revisions:
# #
# # ---------------------------------------------------------------------------------------------------
# =======================================================================================================

""" Import all the required libraries"""
import pandas as pd
from Logger import logger

class dataPreprocessing:
    '''
        This class is created to do data pre-processing step.
    '''

    def __init__(self):
        """ Initialize log file location """
        self.file_object = './ExecutionLogs/featureEngineering.log'

        """ Initialize logger class for log writing """
        self.log_writer = logger.logger(self.file_object)

    def getDataForCollaborativeFiltering(self):
        '''
           This function is created to perform data cleaning and pre-processing steps for collaborative filtering
        '''

        try:

            self.log_writer.log("featureEngineering.py: Data Pre-processing task is started...")

            # Read Order data into dataframe
            df_Order = pd.read_csv('./RawDataFromDB/Order.csv', usecols=['customer_id', 'order_id'])

            # Read Order Item data into dataframe
            df_Order_Item = pd.read_csv('./RawDataFromDB/Order_Item.csv', usecols=['product_id', 'order_id'])

            # Read Product Review data into dataframe
            df_Product_Review = pd.read_csv('./RawDataFromDB/Product_Review.csv', usecols=['product_id', 'ratings'])

            # Combine Order and Order Item data
            df_order_order_item = df_Order.merge(df_Order_Item, on='order_id')

            # Combine Order, order item and Product review data
            df = df_order_order_item.merge(df_Product_Review, on='product_id') # Change the join ket to customer_id once data available

            # Remove Duplicate data
            df.drop_duplicates(subset=['customer_id', 'product_id'], keep='first', inplace=True)

            df.to_csv('./TransformedData/Product_rating_by_Customer.csv', index=False)

            self.log_writer.log("featureEngineering.py: Data Pre-processing task is completed Successfully...")

        except Exception as e:
            # Logging Unsuccessful Execution
            self.log_writer.log("featureEngineering.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of Data Pre-processing task ...")
