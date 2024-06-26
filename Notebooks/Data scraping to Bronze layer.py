# Databricks notebook source
# MAGIC %pip install httpx
# MAGIC %pip install selectolax

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import httpx
import time
from selectolax.parser import HTMLParser

# COMMAND ----------

def get_correct_encoding(response):
    encoding = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
    return encoding or response.apparent_encoding

# COMMAND ----------

def get_html(session, url):
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9,bg;q=0.8'}
    
    response = session.get(url, headers=headers, follow_redirects=True)
    time.sleep(2)

    correct_encoding = get_correct_encoding(response)
    content = response.content.decode(correct_encoding)

    return HTMLParser(content)

# COMMAND ----------

def get_estates_details_links(session):
    steps_to_stop = 1

    estates_list = []

    url = "https://prian.info/search/?where[1]=Bulgaria"

    while url != '' and steps_to_stop > 0:
        #steps_to_stop  -= 1                           # ---------------------------------- Comment to get all estates
        html = get_html(session, url)

        if  html.css_matches('ul.pagination.pagination-square li a[title="Next"]'):
            url = 'https:' + html.css_first('ul.pagination.pagination-square li a[title="Next"]').attrs['href']
        else:
            url = ''
        
        for estate in html.css('div.obj-min.pr-js-object-slider'):
            #print(estate.css_first('div.wrap a').attrs['href'])
            estates_list.append('https:' + estate.css_first('div.wrap a').attrs['href'])

    return estates_list


# COMMAND ----------

def get_estates_details(estate_links, session):
    estates_details = []

    for link in estate_links:
        html = get_html(session, link)
        datails = html.css('tr.c-params__row')

        estate = {}
        estate['source'] = link

        for detail in datails:
            if detail.css_matches('td.c-params__key'):
                key = detail.css_first('td.c-params__key').text(strip=True).strip().replace('\n', '')

                value = ''
                if  detail.css_matches('td.c-params__value'):
                    value = detail.css_first('td.c-params__value').text(strip=True).strip().replace('\n', '')
                else:
                    value = True
                    print(link + ' - ' + key)

                estate[key] = value

        estates_details.append(estate)

    return estates_details

# COMMAND ----------


session = httpx.Client()

estates_links = get_estates_details_links(session)

estates_details = get_estates_details(estates_links, session)

session.close()

# COMMAND ----------

df=spark.createDataFrame(data=estates_details)
df.write.mode('overwrite').option("header",True).csv('/mnt/bronze/csv_estate_2')
df.coalesce(1).write.mode('overwrite').option("header",True).csv("/mnt/bronze/csv_estate_2_single_file")

# COMMAND ----------

df1 = spark.read.option("header", True).csv('/mnt/bronze/csv_estate_2')
display(df1)

