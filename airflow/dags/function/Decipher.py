from decipher.beacon import api
import logging
import pandas as pd

# Set logging to debug to view logging messages in terminal
#logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class decipher_interface:

    decipher_api = api
    #token = "2931bn7g3pwu0ax7wxt9xxpec0bvhm9bffmkjwd7vprq7fm685jz3yntbbrj4bnt"

    def __init__(self, token, url="https://selfserve.decipherinc.com"):
        """Initializes the Decipher API

        **Attributes**::
            * **token**: (str) -> A string value that will be used to authenticate
                with the Decipher API
            * **url**: (str) -> A string value that will be used to set the base
                url for the Decipher API
        """

        logger.info('Authenticating with Decipher API...')

        self.decipher_api.login(token, url)

        logger.info('Decipher API Initialized')

        self._questions = []

    def get_hello_world(self, value):
        """Returns a string with the value passed in
        **Attributes**::
            * **value**: (str) -> A string value that will be returned in the response

        **Returns**::
            * **response**: (str) -> A response from the server
        """

        logger.info(f"GET /hello?name={value}")
        try:
            response = self.decipher_api.get(f"/hello?name={value}")
        except Exception as e:
            logger.error(e)
            return None

        logger.info(f"Response: {response}")

        return response

    def get_surveys(self, scope):
        """Returns a list of surveys owned by the user
        **Attributes**::
            * **scope**: (str) -> Self for surveys owned by you, all for all surveys
                in the organization

        **Returns**::
            * **response**: (list) -> A list of surveys and metadata about them
        """

        logger.info(f"GET rh/companies/{scope}/surveys")
        try:
            response = self.decipher_api.get(f"rh/companies/{scope}/surveys")
        except Exception as e:
            logger.error(e)
            return None

        #logger.info(f"Response: {response}")

        return response

    def get_survey_questions(self, survey_path):
        """Returns a list of survey questions with their datamap (representation of survey
            questions) for a specific survey.
        **Attributes**::
            * **survey_path**: (str) -> The path of the survey you want to retrieve data for. It
                should look something like: *selfserve/1a/123456*. This can be obtained from
                querying for all surveys and using the *path* key
        **Returns**::
            * **response**: (list) -> Survey questions and ther datamap
        """
        path = f'surveys/{survey_path}/datamap?format=json'
        logger.info(f"GET {path}")
        try:
            
            response = self.decipher_api.get(path)['variables']
        except Exception as e:
            logger.error(e)
            return None
        
        #logger.info(f"Response: {response}")

        return response

    def create_survey_question_dataframe(self, questions_array):
        """Creates a Pandas data frame containing information about the questions in a specific
        survey and their datamap
        **Attributes**:
            * **questions_array**: (*array*) A list of dictionaries containing survey questions and
                their datamap

        **Returns**::
            * **df**: (data frame) -> A pandas data frame with survey questions and their datamap
        """
        # Put json data into pandas data frame and additionally normalize the *variables* column
        df = pd.json_normalize(questions_array, record_path=['variables'])
        # Unlist the *values* column
        df = df.explode("values")
        # Create a data frame from the *values* column where each key in the dictionary is a new
        # column
        values_df = df['values'].apply(pd.Series)
        # Combine the two data frames and add a prefix to the values data frame for distinction
        df = pd.concat([df, values_df.add_prefix('values_')], axis=1)
        df.dropna(how='all', axis=1, inplace=True)

        #logger.info(f"Response: {df}")
        return df

    def get_survey_data(self, survey_path, start, end):
        """Returns survey data from a specific survey within the specified date range
        **Attributes**::
            * **survey_path**: (str) -> The path of the survey you want to retrieve data for. It
                should look something like: *selfserve/1a/123456*. This can be obtained from
                querying for all surveys and using the *path* key
            * **start**: (str) -> A datetime to start the query in the format yyyy-mm-ddThh:mmZ
            * **end**: (str) -> A datetime to end the query in the format yyyy-mm-ddThh:mmZ

        **Returns**::
            * **response**: (list) -> A list of survey responses and metadata about them
        """
        path = f'surveys/{survey_path}/data?format=json&start={start}&end={end}'
        logger.info(f"GET {path}")
        try:
            response = self.decipher_api.get(path)
        except Exception as e:
            logger.error(e)
            return None

       # logger.info(f"Response: {response}")

        return response

    def create_dataframe(self, list):
        """Creates a Pandas data frame from a list of dictionaries
        **Attributes**:
            * **list**: (*array*) A list of dictionaries

        **Returns**::
            * **df**: (data frame) -> A pandas data frame with survey data
        """
        if list:
            df = pd.json_normalize(list)

            return df

        else:
            return None