import uuid
import logging
from scrapers import PipelineScraper
from scrapy.http import HtmlResponse
from datetime import date, timedelta
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

    def get_payload(self, cycle: int = None, post_date=None):
        response = self.session.get(self.get_url)
        new_response = HtmlResponse(url="my HTML string", body=response.text, encoding='utf-8')

        __VIEWSTATE = new_response.css("#__VIEWSTATE::attr(value)").extract_first("")
        __EVENTARGUMENT = new_response.css("#__EVENTARGUMENT::attr(value)").extract_first("")
        __EVENTTARGET = new_response.css("#__EVENTTARGET::attr(value)").extract_first("")
        __VIEWSTATEGENERATOR = new_response.css("#__VIEWSTATEGENERATOR::attr(value)").extract_first("")
        __EVENTVALIDATION = new_response.css("#__EVENTVALIDATION::attr(value)").extract_first("")

        form_data = {
            'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$DownloadDDL': 'EXCEL',
            'WebSplitter1_tmpl1_ContentPlaceHolder1_dtePickerBegin_clientState': '|0|01' + post_date + '-0-0-0-0||[[[[]],[],[]],[{},[]],"01' + post_date + '-0-0-0-0"]',
            '__EVENTTARGET': __EVENTTARGET,
            '__EVENTARGUMENT': __EVENTARGUMENT,
            '__VIEWSTATE': __VIEWSTATE,
            '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
            '__EVENTVALIDATION': __EVENTVALIDATION,
            '__ASYNCPOST': 'true',
            'WebSplitter1_tmpl1_ContentPlaceHolder1_ddlCycleDD_clientState': '|0|&tilda;2||[[[[null,null,null,null,null,null,null,-1,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"TIMELY",null,null,null,null,null,null,null,null,null,null,null,null,null,0,0,null,null,1,null,null,null,null,null,null,null,null]],[],null],[{"0":[41,'+str(cycle)+'],"1":[7,'+str(cycle)+'],"2":[23,"EVENING"]},[{"0":[1,0,17],"1":["2",0,81],"2":[1,9,0],"3":["2",9,1],"5":["2",7,1],"6":[1,7,0]}]],null]',
            'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.x': '50',
            'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.y': '11',
        }
        return form_data

    def start_scraping(self, cycle=None, post_date=None):
        try:
            post_date = post_date if post_date is not None else date.today()
            logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)

            payload = self.get_payload(cycle, post_date.strftime('%Y-%-m-%d'))

            response = self.session.post(self.post_data_url, data=payload, headers=self.post_page_headers)
            response.raise_for_status()
            print(response.headers)
            print(vars(response).keys())
            raw_file = response.headers.get('content-disposition')
            filename = raw_file[raw_file.find("filename="):].replace("filename=", "")

            with open(filename, "wb") as file:
                file.write(response.content)
            df = pd.read_excel(filename, engine='openpyxl')
            new_data_frame = self.convert_excel(filename)
            self.save_result(new_data_frame, post_date, local_file=True)
            if os.path.isfile(filename):
                os.remove(filename)



        except Exception as ex:
            logger.error(ex, exc_info=True)

    def convert_excel(self, filename):
        data = pd.read_excel(filename, engine='openpyxl')
        data2 = pd.read_excel(filename, engine='openpyxl', skiprows=3)

        columns2 = data2.columns.ravel()
        columns = data.columns.ravel()
        final_columns = []

        for info in columns:
            if "Unnamed" not in info:
                final_columns.append(info)

        for col in columns2:
            final_columns.append(col)

        count = 0
        raw_dict = {}
        for name in data.iterrows():
            if count == 0:
                raw_dict = name[1]
            count = count + 1

        header_dict = {}
        for key in raw_dict.keys():
            if "Unnamed" not in key:
                header_dict[key] = raw_dict[key]

        list_data = []

        for info in data2.iterrows():
            dict_data = info[1].to_dict()

            if str(dict_data["Loc Name"]) != "nan":
                # for header_info in header_dict:
                final_dict = {**header_dict, **dict_data}
                list_data.append(final_dict)

        df = pd.DataFrame(columns=final_columns)

        counter = 1
        for data in list_data:
            df.loc[counter] = data.values()
            counter = counter + 1
        return df


def back_fill_pipeline_date():
    scraper = Kindermorgan(job_id=str(uuid.uuid4()))
    custom_cycle = 2
    # set desired cycle: TIMELY=1,EVENING=2, INTRADAY 1=3, INTRADAY 2=4,INTRADAY 3=5

    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        scraper.start_scraping(post_date)


def main():
    custom_cycle = 2
    # set desired cycle: TIMELY=1,EVENING=2, INTRADAY 1=3, INTRADAY 2=4,INTRADAY 3=5
    custom_date = date.fromisoformat('2022-06-30')
    scraper = Kindermorgan(job_id=str(uuid.uuid4()))
    scraper.start_scraping(cycle=custom_cycle, post_date=custom_date)


if __name__ == '__main__':
    main()
