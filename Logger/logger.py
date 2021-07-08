# =============================================================================
# # ---------------------------------------------------------------------------
# # Creation Date: 12-Jun-2021
# # Author: PramodKumar Gupta
# # Script: logger.py
# # Purpose: This script is created for writting logs.
# # Version: 1.0
# # Revisions:
# #
# # ---------------------------------------------------------------------------#
# =============================================================================

""" Importing libraries """
from datetime import datetime


class logger:
    """
    Class: logger

    Functionality: This is created for logging the details of execution.

    """

    def __init__(self, file_object):

        self.file_object = file_object

    def log(self, log_message):

        now = datetime.now()
        self.date = now.date()
        self.current_time = now.strftime("%H:%M:%S")

        self.file_object1 = open(self.file_object, "a+")

        self.file_object1.write(str(self.date) + " " + str(self.current_time) + " : " + log_message + "\n")

        self.file_object1.close()