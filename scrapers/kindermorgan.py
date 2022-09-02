import uuid
from datetime import date, timedelta
import logging
from scrapers import PipelineScraper
from scrapy.http import HtmlResponse
import shutil
import pandas as pd
import os



logger = logging.getLogger(__name__)
LOCAL_DATA_FOLDER = './DATA'
_output_folder = f'{LOCAL_DATA_FOLDER}/scraper_output'

class Kindermorgan(PipelineScraper):
    source = "pipeline2.kindermorgan"
    api_url = "https://pipeline2.kindermorgan.com/"
    post_data_url = "https://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code=RUBY"
    get_url = "https://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code=RUBY"

    post_page_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate,br',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': 'https://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code=RUBY',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        'sec-ch-ua-platform': 'Linux',
        'sec-ch-ua-mobile': '?0',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.api_url, source=self.source)

    def get_payload(self,post_date=None):
        response = self.session.get(self.get_url)
        new_response = HtmlResponse(url="my HTML string", body=response.text, encoding='utf-8')

        __VIEWSTATE = new_response.css("#__VIEWSTATE::attr(value)").extract_first("")
        __EVENTARGUMENT = new_response.css("#__EVENTARGUMENT::attr(value)").extract_first("")
        __EVENTTARGET = new_response.css("#__EVENTTARGET::attr(value)").extract_first("")
        __VIEWSTATEGENERATOR = new_response.css("#__VIEWSTATEGENERATOR::attr(value)").extract_first("")
        __EVENTVALIDATION = new_response.css("#__EVENTVALIDATION::attr(value)").extract_first("")


        form_data = {
            'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$DownloadDDL': 'EXCEL',
            'WebSplitter1_tmpl1_ContentPlaceHolder1_dtePickerBegin_clientState': '|0|01'+post_date+'-0-0-0-0||[[[[]],[],[]],[{},[]],"01'+post_date+'-0-0-0-0"]',
            '__EVENTTARGET': __EVENTTARGET,
            '__EVENTARGUMENT': __EVENTARGUMENT,
            '__VIEWSTATE': __VIEWSTATE,
            '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': __EVENTVALIDATION,
            '__ASYNCPOST': 'true',
            'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.x': '50',
            'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.y': '11',
        }
        return form_data

    def start_scraping(self, post_date=None):
        try:
            logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)
            post_date = date.today()
            payload = self.get_payload(post_date.strftime('%Y-%-m-%d'))

            response = self.session.post(self.post_data_url, data=payload, headers=self.post_page_headers)
            response.raise_for_status()
            print(response.headers)
            print(vars(response).keys())
            raw_file = response.headers.get('content-disposition')
            filename = raw_file[raw_file.find("filename="):].replace("filename=", "")

            with open(filename, "wb") as file:
                file.write(response.content)
            df = pd.read_excel(filename,engine='openpyxl')
            self.save_result(df,post_date,local_file=True)
            if os.path.isfile(filename):
                os.remove(filename)



        except Exception as ex:
            logger.error(ex, exc_info=True)


def back_fill_pipeline_date():
    scraper = Kindermorgan(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        scraper.start_scraping(post_date)


def main():
    scraper = Kindermorgan(job_id=str(uuid.uuid4()))
    scraper.start_scraping()


if __name__ == '__main__':
    main()
