##################################################################################################################################
##############################------Data Processing Pipeline to Transform the Web Traffic Data------##############################
##################################################################################################################################

###---import necessary Python packages
import string
import pandas as pd
import ssl

###---disable default certificate verification to bypass the unexpected error while reading data online
ssl._create_default_https_context = ssl._create_unverified_context

###---config section to specify the data source location and files
location = 'https://public.wiwdata.com/engineering-challenge/data'
files = list(string.ascii_lowercase)

###---create a function data_pipeline to perform the tasks of reading data online, transformation, and writing back a .csv file
def data_pipeline(location, files):

    ###---the section to read these 26 .csv files online and union them to construct the dataframe input
    input = pd.DataFrame()

    for file in files:
        link = location + '/' + file + '.csv'
        temp = pd.read_csv(link)
        if len(input) == 0:
            input = temp
        else:
            input = pd.concat([input, temp])

    ###---perform the transformation on dataframe input and generate the dataframe output
    output = input[['user_id', 'path', 'length']]

    output = output.pivot_table(index='user_id', columns='path', values='length')

    output.reset_index(inplace=True)

    output.fillna(0, inplace=True)

    ###---save the final dataframe output as 'output.csv' in the default location
    output.to_csv('output.csv', index=False)

###---run the fucntion data_pipeline
data_pipeline(location, files)









