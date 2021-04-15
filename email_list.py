# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 14:28:30 2020

@author: mrlat
"""

import re
import requests
from urllib.parse import urlsplit
from collections import deque
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
import datetime
import os
import sys
from pathlib import Path
import glob

def email_list( ent_num, ent_name, original_url):
    start = time.time()
    unscraped = deque([original_url])  
    scraped = set()  
    emails = set()  
    org_netloc =  urlsplit(original_url).netloc
    try:
        while len(unscraped):
            if time.time() >= start + 60:
                return emails
            url = unscraped.popleft()  
            
            parts = urlsplit(url)
            tmp_netloc = parts.netloc
            if org_netloc!=tmp_netloc:
                continue
            
            temp_path =  parts.path
            if "//" in temp_path:
                temp_path = temp_path.replace('//','/')
                
            base_url = "{p.scheme}://{p.netloc}".format(p=parts)
            url = "{p.scheme}://{p.netloc}".format(p=parts)+ temp_path
            if url in scraped:
                continue
                
            scraped.add(url)
    
            if '/' in parts.path:
                path = url[:url.rfind('/')+1]
            else:
                path = url
                
            if len(url.split('/'))>5:
                continue
            
            print("Crawling URL %s" % url)
            try:
                response = requests.get(url, timeout = (5,10))   
            except Exception as e:
                company_logger = logging.getLogger('company_failure')
                company_logger.debug('---'.join(['request error', ent_num, ent_name]))
                company_logger.exception(e)
                continue
            if 'forbidden' in response.text.lower():
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36'}
                try:
                    response = requests.get(url, headers = headers, timeout = (5,10))  
                except Exception as e:
                    company_logger = logging.getLogger('company_failure')
                    company_logger.debug('---'.join(['request error', ent_num, ent_name]))
                    company_logger.exception(e)
                    continue
            
            new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[comnetorg]+", response.text, re.I))
            #new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z0-9]+", response.text, re.I))
            emails.update(new_emails) 
    
            soup = BeautifulSoup(response.text, 'lxml')
            start2 = time.time()
            for anchor in soup.find_all("a"):
                if "href" in anchor.attrs:
                    link = anchor.attrs["href"]
                else:
                    link = ''
    
                if link.startswith('//'):
                    link = base_url + link
    
                elif not link.startswith('http'):
                    link = path + link
    
                if not link.endswith(".gz"):                    
                    if not link in unscraped and not link in scraped:
                        if any(ext in link.lower() for ext in ['contact', 'mail', 'info', 'about']):
                            unscraped.append(link)
                if len(unscraped)>=10:
                    break
                if time.time() >= start2 + 20:
                    break
        return emails
    except Exception as e:
        company_logger = logging.getLogger('company_failure')
        company_logger.debug('---'.join(['function processing error', ent_num, ent_name]))
        company_logger.exception(e)
        return emails
        
    

if __name__ == '__main__':
    current_time = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    LOG_DIR = os.path.join(os.path.dirname("logs/" + current_time + "/"))
    os.makedirs(LOG_DIR, exist_ok=True)
    LOG_FILE = os.path.join(LOG_DIR, 'email_list.log')
    PROCESSED = os.path.join(LOG_DIR, 'processed.log')
    FAILED = os.path.join(LOG_DIR, 'fail.log')
    # Module Level logger
    
    module_logger = logging.getLogger(__name__)
    module_logger.setLevel(logging.DEBUG)
    ## Formatter for stream handler
    stream_formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    ## Stream Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.CRITICAL)
    stream_handler.setFormatter(stream_formatter)
    module_logger.addHandler(stream_handler)
    ## Formatter for file handler
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s')
    ## File Handler for logging
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    module_logger.addHandler(file_handler)
    ## Logger to record comapnys processed by scraper
    company_logger = logging.getLogger('company')
    company_logger.setLevel(logging.INFO)
    ## Formatter for file handler
    company_formatter = logging.Formatter('%(message)s')
    ## File Handler to record companys processed by scrapper
    processed_file_handler = logging.FileHandler(PROCESSED)
    processed_file_handler.setLevel(logging.INFO)
    company_logger.addHandler(processed_file_handler)
    ## Logger to record failures
    company_failure_logger = logging.getLogger('company_failure')
    company_failure_logger.setLevel(logging.DEBUG)
    ## File Handler to record failures
    failure_file_handler = logging.FileHandler(FAILED)
    failure_file_handler.setLevel(logging.DEBUG)
    company_failure_logger.addHandler(failure_file_handler)
    
    try:
        #assert len(sys.argv) == 3, "Wrong number of inputs!" 
        #infile, outfile = sys.argv[1:]
        

        path = r'resources_new' # use your path
        all_files = glob.glob(path + "/*.csv")

        li = []

        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)
            li.append(df)

        #frame = pd.concat(li, axis=0, ignore_index=True)
        output_header = ['ENTERPRISE_NBR','COMPANY','FORMER_NAME','NOTES','FEIN','PO_BOX_BLDG1','STREET','PO_BOX_BLDG2','PO_BOX_BLDG3','CITY','STATE','ZIPPOSTAL_CODE','PROVINCE','COUNTRY','PHONE','TOLLFREE','FAX','EMAIL','WEBSITE']
        df = pd.concat([df[output_header] for df in li], ignore_index=True)
        df.to_csv('all_inputs_final.csv', index=False, header = output_header)
        outfile = 'small_business_emails.csv'
        #df = pd.read_csv(infile, dtype = str)
    except AssertionError as e:
        module_logger.critical(e)
        logging.shutdown()
        sys.exit()
    except FileNotFoundError:
        module_logger.critical('Could not find the input file!')
        logging.shutdown()
        sys.exit()
    except pd.errors.ParserError as e:
        module_logger.critical('Error while parsing input file. See the log for more info..')
        module_logger.exception(e)
        logging.shutdown()
        sys.exit()
    except Exception as e:
        module_logger.critical('Error!')
        module_logger.exception(e)
        logging.shutdown()
        sys.exit()
    try:
        
        df = df.fillna(str(0)) 
        df = df.astype(str)
        output_header = ['ENTERPRISE_NBR','COMPANY','FORMER_NAME','NOTES','FEIN','PO_BOX_BLDG1','STREET','PO_BOX_BLDG2','PO_BOX_BLDG3','CITY','STATE','ZIPPOSTAL_CODE','PROVINCE','COUNTRY','PHONE','TOLLFREE','FAX','EMAIL','WEBSITE', 'Email']
        #output_header = ['ID', 'ENTERPRISE_NBR', 'COMPANY', 'STATE', 'ZIPPOSTAL_CODE', 'YEAR_FOUNDED', 'NAICS6', 'WEBSITE', 'EMAIL']
        # Creating the output file
        outpath = Path(outfile)
        os.makedirs(outpath.parent, exist_ok = True)
        with open(outfile, mode='w') as csvfile:
            csvfile.write(','.join(output_header)+'\n')
        for _, row in df.iterrows():
            ent_num = row[0]
            ent_name = row[1]
            ent_fr_name = row[2]
            ent_notes = row[3]
            ent_fein = row[4]
            ent_bld1 = row[5]
            ent_st = row[6]
            ent_bld2 = row[7]
            ent_bld3 = row[8]
            ent_city = row[9]
            ent_state = row[10]
            ent_zip = row[11]
            ent_pro = row[12]
            ent_cntry = row[13]
            ent_phone = row[14]
            ent_tf = row[15]
            ent_fax = row[16]
            ent_email = row[17]
            ent_site = row[18]
            
            if not ent_site.startswith('www'):
                ent_site = 'www.'+ ent_site
            ent_site = 'http://' + ent_site
            out_emails = email_list( ent_num, ent_name, ent_site)
            if len(out_emails)>0:   
                tmp = []
                for mail in list(out_emails):
                    tmp.append([ent_num, ent_name, ent_fr_name, ent_notes,  ent_fein, ent_bld1, ent_st, ent_bld2, ent_bld3, ent_city, ent_state, ent_zip, ent_pro, ent_cntry,  ent_phone,  ent_tf, ent_fax, ent_email, ent_site, mail])
                    
                df = pd.DataFrame(tmp)
                df.to_csv(outfile, mode = 'a', index=False, header = False)
            company_logger.info(row[0])
    except Exception as e:
        module_logger.critical('Error!')
        module_logger.exception(e)
    finally:
        logging.shutdown()
