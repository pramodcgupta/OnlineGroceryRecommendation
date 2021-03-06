# =======================================================================================================
# # ---------------------------------------------------------------------------------------------------
# # Creation Date: 23-Jun-2021
# # Author: PramodKumar Gupta
# # Script: PredictFromModel.py
# # Purpose: This script is created to get predictions from the Model.
# # Version: 1.0
# # Revisions:
# #
# # ---------------------------------------------------------------------------------------------------
# =======================================================================================================

""" Import all the required libraries"""
from ModelCreation import ModelCreation
import pickle
from Logger import logger
import pandas as pd

class getPrediction:
    '''
        This class is created to get predictions from recommendation systems.
    '''

    def __init__(self):
        """ Initialize log file location """
        self.file_object = './ExecutionLogs/PredictFromModel.log'

        """ Initialize logger class for log writing """
        self.log_writer = logger.logger(self.file_object)

    def loadSavedModel(self):
        '''
            This function is created to save the KNN Model
        '''

        try:
            self.log_writer.log("PredictFromModel.py: KNN Model saving task is started...")
            filename = "./Models/Model_KNN.pkl"

            # Load the Model back from file
            with open(filename, 'rb') as file:
                knn_Model = pickle.load(file)

            self.log_writer.log("PredictFromModel.py: KNN Model saving task is completed...")

            return knn_Model

        except Exception as e:
            # Logging Unsuccessful Execution
            self.log_writer.log("PredictFromModel.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of KNN Model saving task ...")

    def getPrediction(self, product_id):
        '''
            This function is created to get predictions from recommendation systems
        '''

        try:
            self.log_writer.log("PredictFromModel.py: Product Recommendation (prediction) task  is started...")

            # Read product data into dataframe
            df_product = pd.read_csv('./RawDataFromDB/Product.csv', usecols=['product_id','product_category_id','product_name','product_price','product_qty','product_image_path'])

            ## get User Interaction Matrix
            createmodel_obj = ModelCreation.createModels()
            df_User_product_mat = createmodel_obj.getUserInteractionMatrix()

            ## Load the KNN Model into Object
            recommendation_model = self.loadSavedModel()

            query_index = df_User_product_mat.index.get_loc(product_id)

            distances, indices = recommendation_model.kneighbors(df_User_product_mat.iloc[query_index, :].values.reshape(1, -1), n_neighbors=6)

            lst=[]

            for i in range(0, len(distances.flatten())):
                if i == 0:
                    pass

                else:
                    lst.append(df_product[df_product.product_id == df_User_product_mat.index[indices.flatten()[i]]].set_index('product_id').to_dict(orient="index"))

            self.log_writer.log("PredictFromModel.py: Product Recommendation (prediction) task is completed...")

            return lst

        except Exception as e:
            # Logging Unsuccessful Execution
            self.log_writer.log("PredictFromModel.py: *** Error Occured ***: " + str(e))
            self.log_writer.log("*** Unsuccessful, End of Product Recommendation (prediction) task ...")