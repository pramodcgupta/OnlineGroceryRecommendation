# =============================================================================
# # ---------------------------------------------------------------------------
# # Creation Date: 12-Jun-2021
# # Author: PramodKumar Gupta
# # Script: collectTrainingData.py
# # Purpose: This script is created for data ingestion layer.
# # Version: 1.0
# # Revisions:
# #
# # ---------------------------------------------------------------------------#
# =============================================================================

""" Import all the required libraries"""
from Logger import logger
from CollectDataFromDB import downloadDataFromMongoDB

class dataIngestion:
    '''
    This class is created to collect training data from MongoDb and download the data locally for furher processing.
    '''

    def __init__(self):
        """ Initialize log file location """
        self.file_object = './ExecutionLogs/collectTrainingData.log'

        """ Initialize logger class for log writing """
        self.log_writer = logger.logger(self.file_object)

    def collectTrainingData(self):
        '''
            This function is created to established connection with MongoDb and download the collections from the Atlas Cloud.
        '''

        try:

            self.log_writer.log("collectTrainingData.py: Training Data Collection task is started...")

            self.log_writer.log("collectTrainingData.py: MongoDB Connection is requested...")

            db_object = downloadDataFromMongoDB.dBOperation()
            client = db_object.connectToMongoDB()

            self.log_writer.log("collectTrainingData.py: MongoDB Connection is established...")

            self.log_writer.log("collectTrainingData.py: MongoDB Collection/table Downloading is started...")

            db_object.downloadDataFromDB(client)

            self.log_writer.log("collectTrainingData.py: MongoDB Collection/table Downloading is completed successfully...")

            self.log_writer.log("collectTrainingData.py: Training Data Collection task is Completed Successfully...")

        except Exception as e:
            # Logging Unsuccessful Execution
            self.log_writer.log("collectTrainingData.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of Training Data Collection task ...")
