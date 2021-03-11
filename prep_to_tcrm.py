from simple_salesforce import Salesforce
import requests
from base64 import b64encode
import os
import pandas as pd
import re
import json
import math
import tempfile

def prep_to_tcrm(df):
    df_temp=df.copy()
    # Salesforce conn info
    security_token = "6ae4ST3kFGQEUoE3DisFrs3J"
    consumer_key = '3MVG9xB_D1giir9oKlQCH3uMIELQE50kPPiANKP3RndPH3v77KL0zwyO._KpS4bqmPICXondVWNDs87EmQHDR'
    consumer_secret = '4EA67316F88C82F5F32680AD61B3A7DD52A6807E74DD091F0138FDAEE0DD2479'
    access_token = '6Cel800D3z0000034Z8S8883z000001HwrtULzHb07deKAkGsSJnkAKb0d6o4QmuLZN3bzEg6llOi4qoQa2NDsOiABfIZy1Eahs2knnnODx'
    sf_username = "arosseatraining@tableaujunkie.com"
    sf_password = " 3mM9^Ms&"
    sf_url = "https://login.salesforce.com"

    # set up some variables
    debug = True
    errlog = "errorlog.txt"
    fname = tempfile.NamedTemporaryFile().name
    metadata = tempfile.NamedTemporaryFile().name

    # set parameters for SFDC login
    params = {
        "grant_type": "password",
        "client_id": consumer_key,  # Consumer Key
        "client_secret": consumer_secret,  # Consumer Secret
        "username": sf_username,  # The email you use to login
        "password": sf_password + security_token  # Concat your password and your security token
    }

    ##PASTE STANDALONE.PY CODE HERE###
    # setup TCRM dataset and app name parameters
    try:
        if 'tcrmdataset' in df_temp:
            EdgemartAlias = re.sub(r'[^a-zA-Z0-9=]+', '', df_temp['tcrmdataset'].values[0].strip())
            EdgemartLabel = re.sub(r'[^a-zA-Z0-9=]+', '', df_temp['tcrmdataset'].values[0].strip())
        else:
            EdgemartLabel = "TableauPrep"  # dataset name
            EdgemartAlias = "TableauPrep"  # dataset alias
        if 'tcrmapp' in df_temp:
            EdgemartContainer = re.sub(r'[^a-zA-Z0-9=]+', '', df_temp['tcrmapp'].values[0].strip())
        else:
            EdgemartContainer = ""  # app name
    except Exception as e:
        if debug: print('Error setting tcrmdataset and tcrmapp parameters')
        with open(errlog, "wb") as errlg:
            errlg.write("Error setting tcrmdataset and tcrmapp parameters.")
            errlg.write(e)
            errlg.flush()
            exit()

    # write temp file and convert dates to consistent format
    for col in df_temp.columns:
        if df_temp[col].dtype == 'object':
            try:
                df_temp[col] = df_temp[col].str.rstrip('Z[UTC]')
                df_temp[col] = df_temp[col].str.rstrip('Z')
                df_temp[col] = pd.to_datetime(df_temp[col], errors='ignore')
            except ValueError:
                pass

    for dtype in df_temp.dtypes.iteritems():
        if dtype[1] == '<M8[ns]':
            if debug: print('Converting date field ', dtype[0])
            df_temp_formatted = df_temp[dtype[0]].dt.strftime("%Y-%m-%d %H:%M:%S")
            df_temp[dtype[0]] = df_temp_formatted

    df_temp.to_csv(fname, mode='w',
              index=False)  # remove encoding='ansi' due to error 'mbcs' codec can't encode characters in position 0--1:

    # ----------------------------
    # Create Fields For TCRM Metajson file
    # ----------------------------
    def create_fields(fname):
        fields = []

        df_temp = pd.read_csv(fname)
        for col in df_temp.columns:
            if df_temp[col].dtype == 'object':
                try:
                    df_temp[col] = pd.to_datetime(df_temp[col], errors='ignore')
                except ValueError:
                    pass

        for dtype in df_temp.dtypes.iteritems():
            if dtype[1] == 'int64':
                fields.append({"fullyQualifiedName": re.sub(r'[^a-zA-Z0-9=]+', '', dtype[0].strip()),
                               "name": re.sub(r'[^a-zA-Z0-9=]+', '', dtype[0].strip()), "type": "Numeric",
                               "label": dtype[0], "precision": 18, "defaultValue": "0", "scale": 0, "format": "0"})
            elif dtype[1] == 'float64':
                fields.append({"fullyQualifiedName": re.sub(r'[^a-zA-Z0-9=]+', '', dtype[0].strip()),
                               "name": re.sub(r'[^a-zA-Z0-9=]+', '', dtype[0].strip()), "type": "Numeric",
                               "label": dtype[0], "precision": 18, "defaultValue": "0", "scale": 4, "format": "0.####",
                               "decimalSeparator": "."})
            elif dtype[1] == '<M8[ns]':
                fields.append({"fullyQualifiedName": re.sub(r'[^a-zA-Z0-9=]+', '', dtype[0].strip()),
                               "name": re.sub(r'[^a-zA-Z0-9=]+', '', dtype[0].strip()), "type": "Date",
                               "label": dtype[0], "format": "yyyy-MM-dd HH:mm:ss"})
            else:
                fields.append({"fullyQualifiedName": re.sub(r'[^a-zA-Z0-9=]+', '', dtype[0].strip()),
                               "name": re.sub(r'[^a-zA-Z0-9=]+', '', dtype[0].strip()), "type": "Text",
                               "label": dtype[0]})
        return fields

    # ----------------------------
    # Split function found here: https://stackoverflow.com/questions/30947682/splitting-a-csv-file-into-equal-parts
    # ----------------------------
    def split(infilename, num_chunks):
        READ_BUFFER = 2 ** 13
        in_file_size = os.path.getsize(infilename)
        if debug: print('SPLIT() in_file_size:', in_file_size)
        chunk_size = in_file_size / num_chunks
        if debug: print('SPLIT(): target chunk_size:', chunk_size)
        files = []
        with open(infilename, 'rb', READ_BUFFER) as infile:
            for _ in range(num_chunks):
                temp_file = tempfile.TemporaryFile()
                while temp_file.tell() < chunk_size:
                    try:
                        temp_file.write(next(infile))
                    except StopIteration:  # end of infile
                        break
                temp_file.seek(0)  # rewind
                files.append(temp_file)
        return files

    ###MAIN PROGRAM###
    # generate metadata json file
    try:
        file = os.path.basename(fname)
        if debug: print('Generating metadata json file using ', file)
        metadata_json = {"fileFormat": {"charsetName": "UTF-8", "fieldsDelimitedBy": ",", "fieldsEnclosedBy": "\"",
                                        "linesTerminatedBy": "\r\n"}, "objects": [
            {"connector": "CSV", "fullyQualifiedName": file.replace('.', '_'), "label": file,
             "name": file.replace('.', '_')}]}
        fields = create_fields(fname)
        metadata_json['objects'][0]['fields'] = fields

        with open(metadata, "w", encoding="utf8") as file:
            json.dump(metadata_json, file)
    except Exception as e:
        if debug: print('Error generating metadata json file')
        with open(errlog, "wb") as errlg:
            errlg.write("Error generating metadata json file.")
            errlg.write(e)
            errlg.flush()
            exit()

    try:
        # make the request and get the access_token and instance_url for future posts
        r = requests.post(
            sf_url + "/services/oauth2/token", params=params)
        # store the tocken and instance url
        access_token = r.json().get("access_token")
        instanceUrl = r.json().get("instance_url")
        if debug: print('Login to Salesforce : access token is ', str(access_token))
    except Exception as e:
        if debug: print('Error in logging into Salesforce.')
        with open(errlog, "wb") as errlg:
            errlg.write("Error posting Auth request to Salesforce.")
            errlg.write(e)
            errlg.flush()
            exit()

    # instantiate the sf object for easy crud operations
    sf = Salesforce(instance_url=instanceUrl, session_id=access_token)

    # set up the data header, by including the data description
    with open(metadata, "r", encoding="utf8") as mdata:
        mdata_contents = b64encode(mdata.read().encode('utf-8'))

    # insert the header record
    params = {
        'Format': 'Csv',
        'EdgemartAlias': EdgemartAlias,
        'EdgemartLabel': EdgemartLabel,
        'Description': 'Tableau Prep API load.',
        'FileName': 'TableauPrep',
        'EdgemartContainer': EdgemartContainer,
        'MetadataJson': mdata_contents.decode(),
        'Operation': 'Overwrite',
        'Action': 'None'
    }
    try:
        res_header = sf.InsightsExternalData.create(params)
        # retrieve the new header id for use with the data parts
        header_id = res_header.get('id')
        if debug: print('Created data header. Id is ', str(header_id))
    except Exception as e:
        if debug: print('Error in writing data header.')
        with open(errlog, "wb") as errlg:
            errlg.write("Error writing data header to Salesforce.")
            errlg.write(str(e))
            errlg.flush()
            exit()

    # if the file is larger than 10mb,
    # it needs to be broken up in chunks
    fsize = os.stat(fname).st_size

    try:
        if (fsize > 10000000):
            if debug: print('File needs to be chunked, size is : ', str(fsize))
            num_chunks = int(math.ceil(float(fsize) / float(10000000)))
            files = split(fname, num_chunks)
            if debug: print('Number of files created: ', format(len(files)))
            for i, ifile in enumerate(files, start=1):
                if debug: print('uploading file ', str(i))
                f_contents = b64encode(ifile.read())
                res_data = sf.InsightsExternalDataPart.create({
                    'DataFile': f_contents.decode(),
                    'InsightsExternalDataId': header_id,
                    'PartNumber': str(i)
                })
                if debug: print('The data part created is : ', str(res_data.get('id')))
        else:
            if debug: print('File is fine to post in single part.')
            # base64 encode the data file
            with open(fname, encoding="utf8") as f:
                f_contents = b64encode(f.read().encode('utf-8'))
                res_data = sf.InsightsExternalDataPart.create({
                    'DataFile': f_contents.decode(),
                    'InsightsExternalDataId': header_id,
                    'PartNumber': '1'
                })
            if debug: print('The data part created is : ', str(res_data.get('id')))
    except Exception as e:
        if debug: print('Error in writing data part.')
        with open(errlog, "wb") as errlg:
            errlg.write("Error writing data part to Salesforce.")
            errlg.write(str(e))
            errlg.flush()
            exit()

    try:
        res_proc = sf.InsightsExternalData.update(header_id, {
            'Action': 'Process'
        })
        if debug: print('The result of the processing the data is : ', str(res_proc))
    except Exception as e:
        if debug: print('Error in Updating action of data header.')
        with open(errlog, "wb") as errlg:
            errlg.write("Error processing data in Salesforce.")
            errlg.write(str(e))
            errlg.flush()
            exit()

    return df
