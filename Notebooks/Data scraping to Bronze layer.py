# Databricks notebook source
# MAGIC %md
# MAGIC Install packages needed for web scraping.

# COMMAND ----------

# MAGIC %pip install httpx
# MAGIC %pip install selectolax

# COMMAND ----------

# MAGIC %md
# MAGIC Restart Python process in order to update after packages installation.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Import necessary packages.

# COMMAND ----------

import httpx
import time
from selectolax.parser import HTMLParser
from typing import Any

# COMMAND ----------

# MAGIC %md
# MAGIC Find correct encoding of the response so it can me read correct as HTML.

# COMMAND ----------

def get_correct_encoding(response: httpx.Response) -> Any:
    encoding: str | None = response.encoding if 'charset' in response.headers.get('content-type', '').lower() else None
    return encoding or response.apparent_encoding

# COMMAND ----------

# MAGIC %md
# MAGIC Read the HTML of the given url and returns HTMLParser.

# COMMAND ----------

def get_html(session: httpx.Client, url: str) -> HTMLParser:
    
    headers: dict = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9,bg;q=0.8'}
    
    response: httpx.Response = session.get(url, headers=headers, follow_redirects=True)
    time.sleep(2)

    correct_encoding: Any = get_correct_encoding(response)
    content: str = response.content.decode(correct_encoding)

    return HTMLParser(content)

# COMMAND ----------

# MAGIC %md
# MAGIC Going through site pagination and get all real estates details urls.

# COMMAND ----------

def get_estates_details_links(session: httpx.Client) -> list:
    steps_to_stop: int = 1

    estates_list: list = []

    url: str = "https://prian.info/search/?where[1]=Bulgaria"

    while url != '' and steps_to_stop > 0:
        steps_to_stop  -= 1                           # ---------------------------------- Comment to get all estates
        html: HTMLParser = get_html(session, url)

        if  html.css_matches('ul.pagination.pagination-square li a[title="Next"]'):
            url = 'https:' + html.css_first('ul.pagination.pagination-square li a[title="Next"]').attrs['href']
        else:
            url = ''
        
        for estate in html.css('div.obj-min.pr-js-object-slider'):
            #print(estate.css_first('div.wrap a').attrs['href'])
            estates_list.append('https:' + estate.css_first('div.wrap a').attrs['href'])

    return estates_list


# COMMAND ----------

# MAGIC %md
# MAGIC Going through the list of all real estates details urls and reads and store in the list of dictionaries the details for each one.

# COMMAND ----------

def get_estates_details(estate_links: list, session: httpx.Client) -> list:
    estates_details: list = []

    for link in estate_links:
        html: HTMLParser = get_html(session, link)
        datails: list = html.css('tr.c-params__row')

        estate: dict = {}
        estate['source'] = link

        for detail in datails:
            if detail.css_matches('td.c-params__key'):
                key: str = detail.css_first('td.c-params__key').text(strip=True).strip().replace('\n', '')

                value: Any = ''
                if  detail.css_matches('td.c-params__value'):
                    value = detail.css_first('td.c-params__value').text(strip=True).strip().replace('\n', '')
                else:
                    value = True
                    print(link + ' - ' + key)

                estate[key] = value

        estates_details.append(estate)

    return estates_details

# COMMAND ----------

# MAGIC %md
# MAGIC Writes the extracted data for the real estates in to the mounted bronze layer as CSV file. Data can be saved as single file or distributed by Spark in several different files.

# COMMAND ----------

def write_CSV_data_to_broze_Layer(data: list, save_as_single_file: bool)-> None:
    df: spark.DataFrame =spark.createDataFrame(data=estates_details)
    if save_as_single_file:
        df.coalesce(1).write.mode('overwrite').option("header",True).csv("/mnt/bronze/csv_estate_2_single_file")
        print('single file')
    else:
        df.write.mode('overwrite').option("header",True).csv('/mnt/bronze/csv_estate_2')
        print('multiple files')
   

# COMMAND ----------

# MAGIC %md
# MAGIC Read, extract and save the data from real estate web site as CSV file in the bronze layer.

# COMMAND ----------


session: httpx.Client = httpx.Client()

estates_links: list = get_estates_details_links(session)

estates_details: list = get_estates_details(estates_links, session)

session.close()

write_CSV_data_to_broze_Layer(estates_details, False)

# COMMAND ----------

# MAGIC %md
# MAGIC Read and show data from bronze layer to be shure that it is writen correct.

# COMMAND ----------

df1 = spark.read.option("header", True).csv('/mnt/bronze/csv_estate_2')
display(df1)

