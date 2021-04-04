import asyncio
import pandas as pd
import aiohttp
from bs4 import BeautifulSoup

texts = []
category = []

async def get_page(session, page_url):
    async with session.get(page_url) as resp:
        return await resp.text()


async def query_pages(pages_urls):
    async with aiohttp.ClientSession() as session:
        tasks = [get_page(session, page_url) for page_url in pages_urls]
        return await asyncio.gather(*tasks)


async def parse_pages(pages):
    global texts

    all_links = []

    for page in pages:
        soup = BeautifulSoup(page, 'html.parser')
        for i in soup.find_all('div', "appeal-element"):
            category.append(i.find('p', 'appeal-cat-title').getText())
        for i in soup.find_all('a', {'class': 'appeal-title-link'}):
            all_links += ['http://zpp.rospotrebnadzor.ru' + i['href']]

    detailed_pages = await query_pages(all_links)

    for detailed_page in detailed_pages:
        soup = BeautifulSoup(detailed_page, 'html.parser')
        texts.append(soup.find('p', "appeal-details-message").getText())


async def main():
    urls = ['http://zpp.rospotrebnadzor.ru/Forum/Appeals/AjaxindexList?page='
            + str(i)
            + '&searchtext=&categories=%5B%5D'
            for i in range(1, 1001)]

    pages = await query_pages(urls)
    await parse_pages(pages)

asyncio.run(main())

df = pd.DataFrame({'text':texts, 'category':category})
df.to_csv('all_texts.csv')
