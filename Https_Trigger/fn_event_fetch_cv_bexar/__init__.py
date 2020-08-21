import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import wget
import tempfile
import glob
import os
from bs4 import BeautifulSoup
import requests
import datetime
import dateutil.parser
import dateutil.relativedelta
conn_string = os.environ["BlobConnectionString"]

#conn_string="DefaultEndpointsProtocol=https;AccountName=jaykakadiya93;AccountKey=3fruF/Sk6K1XebY3M63uAtL0+9AvggPQw3Cg9bnJlf5QKmNlrDwaPv3ft4dY1zcfpOvvHVLKs/zcDRYO+zFHTw==;EndpointSuffix=core.windows.net"
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python HTTP trigger function processed a request.')

        blob_date = req.params.get('dataset_date')
        if not blob_date:
            try:
                req_body = req.get_json()
            except ValueError:
                pass
            else:
                blob_date = req_body.get('dataset_date')

        if blob_date:
            response = requests.get("https://sanantonio.govqa.us/webapp/_rs/(S(iygrpx4c3thz5sbhv2l5knyo))/BusinessDisplay.aspx?sSessionID=&did=9&cat=0")
            soup = BeautifulSoup(response.content,"html.parser")
            soup_link = soup.findAll("div",{"class":"qac_link"})[-1]

            trigger_date = dateutil.parser.parse(blob_date)
            dataset_datetime = trigger_date - dateutil.relativedelta.relativedelta(months=1)
            dataset_date = dataset_datetime.strftime("%Y-%m")
            #cretae tem directory to store the file
            temp_dirpath = tempfile.mkdtemp()
            #url for file
            url = 'https://sanantonio.govqa.us/webapp/_rs/(S(dys2mb00n5fxs0rnwbikqxbs))/'+soup_link.a['href']
            #download the file from site and store to temp directory
            wget.download(url, temp_dirpath)
            file_list = glob.glob(temp_dirpath+"/*.xlsx")
            file_name_split = file_list[0].split("/")
            file_name = file_name_split[-1]

            
            block_blob_service = BlobServiceClient.from_connection_string(conn_string)

            blob_container = "source"
            blob_file_name = "events/cv/tx/bexar/"+dataset_date+"/"+file_name

            blob_client = block_blob_service.get_blob_client(blob_container, blob_file_name)

            downloaded_file = file_list[0]

            with open(downloaded_file, "rb") as f:
                blob_client.upload_blob(f, overwrite=True)
            return func.HttpResponse(f"New Request received! : file path="+blob_file_name)
        else:
            return func.HttpResponse(
                "Please pass a blob_date on the query string or in the request body",
                status_code=400
            )

    except Exception as e:
        print(e)
