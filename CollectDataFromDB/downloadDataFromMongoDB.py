# =============================================================================
# # ---------------------------------------------------------------------------
# # Creation Date: 12-Jun-2021
# # Author: PramodKumar Gupta
# # Script: downloadDataFromMongoDB.py
# # Purpose: This script is created for downloading the data from MongoDB Server.
# # Version: 1.0
# # Revisions:
# #
# # ---------------------------------------------------------------------------#
# =============================================================================

import json
import pymongo
import pandas as pd
from Logger import logger

class dBOperation:
    '''
    This class is created to perform MongoDB related database operations
    '''

    def __init__(self):
        ## Initialize log file location
        self.file_object = './ExecutionLogs/downloadDataFromMongoDB.log'

        ## Initialize logger class for log writing
        self.log_writer = logger.logger(self.file_object)


    def connectToMongoDB(self):
        '''
        This function is written to get connected to MongoDB Cloud Server
        '''

        try:
            self.log_writer.log("downloadDataFromMongoDB.py: MongoDB Data Reading task is started...")

            with open('.config/credential.json') as f:
                credential_dict = json.load(f)

            ## Read the MongoDB Url into variable

            mongodb_url = credential_dict["mongodb_url"]

            ## Generate the mongodb url for connection string
            mongodb_url = mongodb_url.replace('<mongodb_Username>', credential_dict["mongodb_Username"])
            mongodb_url = mongodb_url.replace('<mongodb_Password>', credential_dict["mongodb_Password"])
            mongodb_url = mongodb_url.replace('<mongodb_DBName>', credential_dict["mongodb_DBName"])

            self.log_writer.log("downloadDataFromMongoDB.py: MongoDB Data Reading task is Completed Successfully...")

            return pymongo.MongoClient(mongodb_url)

        except Exception as e:
            # Logging Unsuccessful CSV Reading File
            self.log_writer.log("downloadDataFromMongoDB.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of MongoDB Data Reading task ...")



    def downloadDataFromDB(self, client):
        '''
                This function is written to download the collections from MongoDb and Dump data into csv files.
        '''

        try:

            ## -------------- Customer --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Customer Collection Downloading task is started...")

            ### Get tables into dataframe
            df_Customer = pd.DataFrame.from_records(client.OnlineGroceryDB.Customer.find())
            ### Dropping _id column which is added by MongoDB
            df_Customer.drop(['_id'], axis=1, inplace=True)
            ### Dumping data into csv file.
            df_Customer.to_csv('./RawDataFromDB/Customer.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Customer Collection Downloading task is Completed...")

            ## -------------- Complaint --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Complaint Collection Downloading task is started...")

            df_Complaint = pd.DataFrame.from_records(client.OnlineGroceryDB.Complaint.find())
            df_Complaint.drop(['_id'], axis=1, inplace=True)
            df_Complaint.to_csv('./RawDataFromDB/Complaint.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Complaint Collection Downloading task is Completed...")

            ## -------------- Location --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Location Collection Downloading task is started...")

            df_Location = pd.DataFrame.from_records(client.OnlineGroceryDB.Location.find())
            df_Location.drop(['_id'], axis=1, inplace=True)
            df_Location.to_csv('./RawDataFromDB/Location.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Location Collection Downloading task is Completed...")

            ## -------------- Offer  --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Offer Collection Downloading task is started...")

            df_Offer = pd.DataFrame.from_records(client.OnlineGroceryDB.Offer.find())
            df_Offer.drop(['_id'], axis=1, inplace=True)
            df_Offer.to_csv('./RawDataFromDB/Offer.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Offer Collection Downloading task is Completed...")

            ## -------------- Order --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Order Collection Downloading task is started...")

            df_Order = pd.DataFrame.from_records(client.OnlineGroceryDB.Order.find())
            df_Order.drop(['_id'], axis=1, inplace=True)
            df_Order.to_csv('./RawDataFromDB/Order.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Order Collection Downloading task is Completed...")

            ## -------------- Order_Item --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Order_Item Collection Downloading task is started...")

            df_Order_Item = pd.DataFrame.from_records(client.OnlineGroceryDB.Order_Item.find())
            df_Order_Item.drop(['_id'], axis=1, inplace=True)
            df_Order_Item.to_csv('./RawDataFromDB/Order_Item.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Order_Item Collection Downloading task is Completed...")

            ## -------------- Payment --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Payment Collection Downloading task is started...")

            df_Payment = pd.DataFrame.from_records(client.OnlineGroceryDB.Payment.find())
            df_Payment.drop(['_id'], axis=1, inplace=True)
            df_Payment.to_csv('./RawDataFromDB/Payment.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Payment Collection Downloading task is Completed...")

            ## -------------- Product --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Product Collection Downloading task is started...")

            df_Product = pd.DataFrame.from_records(client.OnlineGroceryDB.Product.find())
            df_Product.drop(['_id'], axis=1, inplace=True)
            df_Product.to_csv('./RawDataFromDB/Product.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Product Collection Downloading task is Completed...")

            ## -------------- Product_Category --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Product_Category Collection Downloading task is started...")

            df_Product_Category = pd.DataFrame.from_records(client.OnlineGroceryDB.Product_Category.find())
            df_Product_Category.drop(['_id'], axis=1, inplace=True)
            df_Product_Category.to_csv('./RawDataFromDB/Product_Category.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Product_Category Collection Downloading task is Completed...")

            ## -------------- Product_Review --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Product_Review Collection Downloading task is started...")

            df_Product_Review = pd.DataFrame.from_records(client.OnlineGroceryDB.Product_Review.find())
            df_Product_Review.drop(['_id'], axis=1, inplace=True)
            df_Product_Review.to_csv('./RawDataFromDB/Product_Review.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Product_Review Collection Downloading task is Completed...")

            ## -------------- Return_Product  --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Return_Product Collection Downloading task is started...")

            df_Return_Product = pd.DataFrame.from_records(client.OnlineGroceryDB.Return_Product.find())
            df_Return_Product.drop(['_id'], axis=1, inplace=True)
            df_Return_Product.to_csv('./RawDataFromDB/Return_Product.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Return_Product Collection Downloading task is Completed...")

            ## -------------- Sellers  --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: Sellers Collection Downloading task is started...")

            df_Sellers = pd.DataFrame.from_records(client.OnlineGroceryDB.Sellers.find())
            # df_Sellers.drop(['_id'], axis=1, inplace=True)
            df_Sellers.to_csv('./RawDataFromDB/Sellers.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: Sellers Collection Downloading task is Completed...")

            ## -------------- User_detail --------------------------
            self.log_writer.log("downloadDataFromMongoDB.py: User_detail Collection Downloading task is started...")

            df_User_detail = pd.DataFrame.from_records(client.OnlineGroceryDB.User_detail.find())
            # df_User_detail.drop(['_id'], axis=1, inplace=True)
            df_User_detail.to_csv('./RawDataFromDB/User_detail.csv', index=False)

            self.log_writer.log("downloadDataFromMongoDB.py: User_detail Collection Downloading task is Completed...")

        except Exception as e:
            # Logging Unsuccessful CSV Reading File
            self.log_writer.log("downloadDataFromMongoDB.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of Downloading Collection task ...")
