# =====================================================================================================================
# # -------------------------------------------------------------------------------------------------------------------
# # Project Objective: Online Grocery Recommendation project
# #
# # Point of Contact: Online Grocery (ML) Team
# # Creation Date: 12-Jun-2021
# # Author: Pramodkumar Gupta
# # Script: main module for recommendation project
# # Purpose: This is main module of the project and used to call all functions associated with the project.
# # Revisions: Initial Version
# #
# =====================================================================================================================
# # -------------------------------------------------------------------------------------------------------------------

""" Import all the required libraries"""
from CollectDataFromDB import collectTrainingData
from Logger import logger
from FeatureEngineering import featureEngineering
from ModelCreation import ModelCreation
from PredictFromModel import PredictFromModel

class recommendation:
    '''
        This class is main method of recommendation project from where all other sub modules are being called.
    '''

    def __init__(self):
        ## Initialize log file location
        self.file_object = './ExecutionLogs/recommendation_main.log'

        ## Initialize logger class for log writing
        self.log_writer = logger.logger(self.file_object)

    def trainingRecommendationEngine(self):
        '''
           This function is created to do Model training for Recommendation system. This will be scheduled for daily run.
        '''

        try:
            self.log_writer.log("recommendation_main.py: ML Model Training is started...")

            ### ----------- Collect training data from MongoDb Cloud Server -----------------
            dataIngestion_obj = collectTrainingData.dataIngestion()
            dataIngestion_obj.collectTrainingData()

            ## -----------  Perform Feature Engineering -------------------------------------
            dataPreprocessing_obj = featureEngineering.dataPreprocessing()
            dataPreprocessing_obj.getDataForCollaborativeFiltering()

            ## -----------  Create KNN Model for recommendation -----------------------------
            createModel_obj = ModelCreation.createModels()
            createModel_obj.modelForCollaborativeFiltering()

            self.log_writer.log("recommendation_main.py: ML Model Training is completed...")

        except Exception as e:
            # Logging Unsuccessful Execution
            self.log_writer.log("recommendation_main.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of ML Model Training task ...")

    def getProductRecommendation(self, product_id):
        '''
            This function is created for getting recommendations for the product on grocery website
        '''

        try:
            self.log_writer.log("recommendation_main.py: Recommendation task is started...")

            self.product_id = product_id

            predict_obj = PredictFromModel.getPrediction()
            predict_obj.getPrediction(self.product_id)

            self.log_writer.log("recommendation_main.py: Recommendation task is completed...")

        except Exception as e:
            # Logging Unsuccessful Execution
            self.log_writer.log("recommendation_main.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of Recommendation task ...")

if __name__ == "__main__":
    ## initialize Class Object for recommendation
    recommend_obj = recommendation()

    ## --------------- Train the recommedation engine --------------------------------
    #recommend_obj.trainingRecommendationEngine()

    ## ---------------- Get the recommendation from the Model ------------------------

    product_id_fromWeb = 'PRO030'
    recommend_obj.getProductRecommendation(product_id_fromWeb)
