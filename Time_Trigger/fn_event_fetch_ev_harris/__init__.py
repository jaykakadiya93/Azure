import datetime
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import wget
import tempfile
import glob
import os
conn_string = os.environ["BlobConnectionString"]


def _download_file_to_temp(court_id):
    #cretae tem directory to store the file
    temp_dirpath = tempfile.mkdtemp()
    #url for file
    url = 'https://jpwebsite.harriscountytx.gov/PublicExtracts/GetExtractData?extractCaseType=CV&extract=1&court='+court_id+'&casetype=EV&format=csv&fdate=08%2F01%2F2020&tdate=08%2F12%2F2020'
    #download the file from site and store to temp directory
    wget.download(url, temp_dirpath)
    file_list = glob.glob(temp_dirpath+"/*.txt")
    file_name_split = file_list[0].split("/")
    file_name = file_name_split[-1]

    block_blob_service = BlobServiceClient.from_connection_string(conn_string)

    blob_container = "source"
    blob_file_name = "events/cv/tx/bexar/"+blob_date+"/"+file_name

    blob_client = block_blob_service.get_blob_client(blob_container, blob_file_name)

    downloaded_file = file_list[0]

    with open(downloaded_file, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')
    
    _download_file_to_temp(305)
    _download_file_to_temp(310)
    _download_file_to_temp(315)
    _download_file_to_temp(320)
    _download_file_to_temp(325)
    _download_file_to_temp(330)
    _download_file_to_temp(335)
    _download_file_to_temp(340)
    _download_file_to_temp(345)
    _download_file_to_temp(350)
    _download_file_to_temp(355)
    _download_file_to_temp(360)
    _download_file_to_temp(365)
    _download_file_to_temp(370)
    _download_file_to_temp(375)
    _download_file_to_temp(380)
    logging.info('Python timer trigger function ran at %s', utc_timestamp)
