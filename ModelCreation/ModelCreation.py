# =======================================================================================================
# # ---------------------------------------------------------------------------------------------------
# # Creation Date: 23-Jun-2021
# # Author: PramodKumar Gupta
# # Script: ModelCreation.py
# # Purpose: This script is created to create recommendation models.
# # Version: 1.0
# # Revisions:
# #
# # ---------------------------------------------------------------------------------------------------
# =======================================================================================================

""" Import all the required libraries"""
import pandas as pd
from sklearn.neighbors import NearestNeighbors
import pickle
from scipy.sparse import csr_matrix
from Logger import logger

class createModels:
    '''
            This class is created models for recommendation systems.
    '''

    def __init__(self):
        """ Initialize log file location """
        self.file_object = './ExecutionLogs/ModelCreation.log'

        """ Initialize logger class for log writing """
        self.log_writer = logger.logger(self.file_object)

    def getUserInteractionMatrix(self):
        '''
            This function is created to get User Interaction Matrix
        '''

        try:
            self.log_writer.log("ModelCreation.py: User Interaction Matrix Creation task is started...")

            # Read Product_rating_by_Customer data into Dataframe
            df_User_product_mat = pd.read_csv('./TransformedData/Product_rating_by_Customer.csv')

            ## Generate User Interaction Matrix
            df_User_interaction_mat = df_User_product_mat.pivot(index='product_id', columns='customer_id',
                                                                values='ratings').fillna(0)

            self.log_writer.log("ModelCreation.py: User Interaction Matrix Creation task is completed Successfully...")

            return df_User_interaction_mat

        except Exception as e:
            # Logging Unsuccessful Execution
            self.log_writer.log("ModelCreation.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of User Interaction Matrix Creation task ...")


    def modelForCollaborativeFiltering(self):
        '''
            This function is created for collaborative filtering KNN Model creation task
        '''

        try:
            self.log_writer.log("ModelCreation.py: Collaborative filtering KNN Model task is started...")

            ## Get the user Interaction Matrix
            df_User_interaction_mat = self.getUserInteractionMatrix()

            ## Initialize KNN Model
            model_knn = NearestNeighbors(metric='cosine', algorithm='brute')
            model_knn.fit(csr_matrix(df_User_interaction_mat.values))

            # dump information to that file
            file = open('./Models/Model_KNN.pkl', 'wb')
            pickle.dump(model_knn, file)

            self.log_writer.log("ModelCreation.py: Collaborative filtering KNN Model task is completed Successfully...")

        except Exception as e:
            # Logging Unsuccessful Execution
            self.log_writer.log("ModelCreation.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of Collaborative filtering KNN Model task ...")
