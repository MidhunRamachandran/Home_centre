from bs4 import BeautifulSoup
import re
import urllib3
import requests
import json
from threading import Thread
import queue
import codecs
import csv
import random
import math

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

concurrent = 1
input_file = r"E:\Home_centre\homecentre_keyword_input_sample.tsv"
pdp_output_file = r"E:\Home_centre\homecentre_keyword_output_sample.tsv"

# concurrent = 3
# input_file = sys.argv[2]
# output_file = sys.argv[3]

proxy_pool = []


def proxy_rotate():
    ip = str(random.choice(proxy_pool))
    return ({"http": "http://" + ip, "https": "https://" + ip})  # Lime


pdp_onfile = codecs.open(pdp_output_file, mode="w", encoding='utf-8')
pdp_onfile.write("UID\tGtin_input\tCategory_url\tProduct_id\tProduct_name\tTaxonomy\tMRP\tSelling_price\tImages\tProduct_url\tRank\tCurrent_page\tReuslt_count\tStatus\n")



def doWork():
    while True:
        attr = q.get()
        # uid = attr['uid']
        # sku_input = attr['sku']
        # gtin_input = attr['gtin']
        # category_url = attr['product_url']
        uid = attr[0]
        sku_input = attr[1]
        gtin_input = attr[2]
        category_url = attr[3]
        getStatus(uid, sku_input, gtin_input, category_url)
        q.task_done()


# Unnecessary whitespaces removal
def write_cat(value):
    field_cat = ''
    for field_iter in value:
        field_iter = re.sub(r'\r+|\t+|\n+|\s+', ' ', str(field_iter).strip()).strip()
        field_iter = re.sub(r'\s+', ' ', field_iter).strip()
        if field_iter == '':
            field_iter = 'n/a'
        field_cat += str(field_iter).strip() + "\t"
    field_cat = str(field_cat).strip() + "\n"
    return field_cat


def make_request(payload):
    headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Length": "767",
            "Content-type": "application/x-www-form-urlencoded",
            "Host": "lm8x36l8la-dsn.algolia.net",
            "Origin": "https://www.homecentre.in",
            "Pragma": "no-cache",
            "Referer": "https://www.homecentre.in/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"
        }
    res_db = []
    source_db = []
    count_db = 0
    url = "https://lm8x36l8la-dsn.algolia.net/1/indexes/*/queries?X-Algolia-API-Key=00495856a8cdc464902a161c87909af7&X-Algolia-Application-Id=LM8X36L8LA&X-Algolia-Agent=Algolia%20for%20vanilla%20JavaScript%202.9.7"
    payload = str(payload).replace('https://www.homecentre.in/in/en/c/','')
    payload = {"requests":[{"indexName":"prod_omp_india_homecentre_Product","params":"query=*&hitsPerPage=42&page=0&facets=*&facetFilters=%5B%22inStock%3A1%22%2C%22approvalStatus%3A1%22%2C%22allCategories%3A{}%22%2C%22badge.title.en%3A-LASTCHANCE%22%5D&getRankingInfo=1&clickAnalytics=true&attributesToHighlight=null&analyticsTags=%5B%22{}%22%2C%22en%22%5D&attributesToRetrieve=concept%2CmanufacturerName%2Curl%2C333WX493H%2C345WX345H%2C505WX316H%2C550WX550H%2C499WX739H%2Cbadge%2Cname%2Csummary%2CwasPrice%2Cprice%2CemployeePrice%2CshowMoreColor%2CproductType%2CchildDetail%2Csibiling%2CthumbnailImg%2CgallaryImages%2CisConceptDelivery&numericFilters=price%20%3E%200&query=*&maxValuesPerFacet=500&tagFilters=%5B%5B%22homecentre%22%5D%5D".format(payload,payload)}]}
    payload = json.dumps(payload)
    while count_db < 5:
        try:
            response = requests.post(url=url, verify=False, data=payload, headers=headers, timeout=10)  # , proxies=proxy_rotate())
            if response.status_code == 200:
                # response = BeautifulSoup(response.text, features='html.parser')
                source_db.append(json.loads(response.content))
                res_db.clear()
                res_db.append(200)
            else:
                res_db.clear()
                res_db.append(response.status_code)

        except Exception as e:
            res_db.clear()
            res_db.append(0)
        if res_db[0] == 200:
            break
        count_db += 1
    if source_db:
        return source_db[0]
    else:
        return 'error'


def trim_ext(string):
    string = re.sub('\s+|\t+|\n+|\r+', ' ', string).strip()
    string = re.sub('\s+', ' ', string).strip()
    return string


def list_cat(value):
    field_cat = ''
    for field_iter in value:
        field_iter = re.sub(r'\s+|\t+|\n+|\r+', ' ', str(value[field_iter]).strip()).strip()
        field_iter = re.sub(r'\s+', ' ', field_iter).strip()
        field_cat += str(field_iter).strip() + "\t"
    field_cat = str(field_cat).strip() + "\n"
    return field_cat


    # pdp_db.append(product_id)
    # pdp_src = [uid,gtin,product_id,product_sku,product_name,mrp,selling_price,discount,size,stock,product_description,product_details,vendor_details,return_exchange_policy,delivery,product_images,url,'n/a']
    # pdp_onfile.write(write_cat(pdp_src))
    #
    # return pdp_db


def getStatus(uid, sku_input, gtin_input, product_page_url_input):

    print('------------------list : {}.{} ------------------'.format(uid, product_page_url_input))
    url = product_page_url_input
    page_source = make_request(url)
    if page_source != 'error':
        page_source = dict(page_source)
        for list_i in page_source['results']:
            if 'hits' in list_i.keys():
                result_count = list_i['nbHits']
                list_i = list_i['hits']
                rank = 1
                for list_data in list_i:

                    rank = rank
                    rank+=1

                    try:
                        product_id = list_data['objectID']
                    except:
                        product_id = 'n/a'

                    try:
                        product_name = list_data['name']['en']
                    except:
                        product_name = 'n/a'

                    try:
                        taxonomy = str(url).replace('https://www.homecentre.in/in/en/c/', '').replace('-', '>')
                    except:
                        taxonomy = 'n/a'

                    try:
                        MRP = list_data['wasPrice']
                    except:
                        MRP = 'n/a'

                    try:
                        selling_price = list_data['price']
                    except:
                        selling_price = 'n/a'

                    try:
                        product_url = "https://www.homecentre.in/in/en{}".format(list(list_data['url'].values())[0]['en'])
                    except:
                        product_url = 'n/a'

                    try:
                        image_db = []
                        for i in list_data['gallaryImages']:
                            img_data = i
                            if image_db:
                                img_data = image_db[0] + '|' + img_data
                                image_db.clear()
                                image_db.append(img_data)
                            else:
                                image_db.append(img_data)
                        images = image_db[0]
                    except:
                        images = 'n/a'

                    reuslt_count = result_count

                    current_page = rank / 20
                    current_page = math.ceil(int(current_page)) + 1

                    pdp_src = [uid, gtin_input,product_page_url_input,product_id,product_name,taxonomy,MRP,selling_price,images,product_url,rank,current_page,reuslt_count,'n/a']
                    pdp_onfile.write(write_cat(pdp_src))

    else:
        pdp_src = [uid, gtin_input, product_page_url_input, 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a', 'n/a','n/a', 'n/a', 'Check network Error']
        pdp_onfile.write(write_cat(pdp_src))


q = queue.Queue(concurrent * 2)
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()

with open(input_file, "r") as tsvin:
    for line in csv.reader(tsvin, dialect='excel-tab'):
        if str(line[0]).lower() != "uid":
            url = line
            q.put(url)
    q.join()
    pdp_onfile.close()

# request_check = 0
# while request_check == 0:
# 	db_url = 'http://localhost:5002/results?db=' + str(input_file)
# 	# db_url = 'http://localhost:5000/results?db=souq_prodlpmuct_input_26apr19'
# 	response = requests.get(db_url)
# 	try:
# 		response = json.loads(response.content)
# 		if not response:
# 			request_check = 1
# 		else:
# 			for sku in response:
# 				q.put(sku)
# 	except:
# 		pass
# q.join()
# onfile.close()
