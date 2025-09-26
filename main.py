#!/usr/bin/env python3
import asyncio
import os
from playwright.async_api import Playwright, async_playwright
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from openai import OpenAI
import json
from kafka import KafkaProducer
from playwright.async_api import Playwright, async_playwright, TimeoutError

load_dotenv() 

AUTH = os.getenv('AUTH')
BASE_URL = 'https://zoopla.co.uk/'
LOCATION = 'London'

def extract_pictures(picture_section):
    pictures_sources = []
    for picture in picture_section.find_all('picture'):
        for source in picture.find_all('source'):
            source_type = source.get('type',"").split('/')[-1]
            pic_url = source.get('srcset','').split(',')[0].split(" ")[0]

            if source_type == 'webp' and '1024' in pic_url:
                pictures_sources.append(pic_url)

    return pictures_sources

def extract_more_infomation(section):
    print("Extracting more infomations...")
    data = {
        "tenure": None,
        "service_charge": None,
        "concil_tax_band": None,
        "commonhold_detail": None,
        "ground_rent": None
    }

    # Find all list items in the key info section
    list_items = section.select('section[aria-labelledby="key-info"] ul._1khto1l1 > li._1khto1l2')

    for item in list_items:
        title_tag = item.find('p', class_='_1khto1l3')
        value_tag = item.find('p', class_='_1khto1l6')

        if title_tag and value_tag:
            title = title_tag.get_text(strip=True).lower().replace(' ', '_')
            value = value_tag.get_text(strip=True)

            if 'tenure' in title:
                data['tenure'] = value
            elif 'service_charge' in title:
                # Fix: Clean the service charge value and convert to integer
                cleaned_value = value.replace('£', '').replace(',', '').replace('per year', '').strip()
                try:
                    data['service_charge'] = int(cleaned_value)
                except (ValueError, TypeError):
                    data['service_charge'] = None
            elif 'council_tax_band' in title:
                data['concil_tax_band'] = value
            elif 'ground_rent' in title:
                data['ground_rent'] = value

    return data

def extract_property_details(input):
    print("Extracting property details...")
    # Initialize a dictionary with all expected keys
    data = {
        "title":None,
        "price": None,
        "address": None,
        "bedrooms": None,
        "bathrooms": None,
        "room_size": None,  
        "receptions": None,
        "EPC_Rating": None,
    }
    # Find the main heading <h1> and the address <address>
    heading_tag = input.find('h1', class_='_1olqsf97')
    address_tag = input.find('address', class_='_1olqsf98')
    
    if heading_tag and address_tag:
        # Combine the text from the heading and the address
        title_text = heading_tag.get_text(strip=True) + " " + address_tag.get_text(strip=True)
        data['title'] = title_text

    # Extract price and clean the value
    price_tag = input.find('p', class_='r4q9to1')
    if price_tag:
        price_text = price_tag.get_text(strip=True).replace('£', '').replace(',', '')
        data['price'] = int(price_text)

    # Extract address
    address_tag = input.find('address', class_='_1olqsf98')
    if address_tag:
        data['address'] = address_tag.get_text(strip=True)

    # Extract number of beds, baths, and receptions
    list_items = input.find('ul', class_='_1wmbmfq1')
    if list_items:
        items = list_items.find_all('p', class_='_1wmbmfq3')
        for item in items:
            text = item.get_text(strip=True)
            if 'bed' in text:
                data['bedrooms'] = text.split()[0]
            elif 'bath' in text:
                data['bathrooms'] = text.split()[0]
            elif 'reception' in text:
                data['receptions'] = text.split()[0]
            elif 'sq. ft' in text:
                data['room_size'] = int(text.split()[0])

    # Extract EPC Rating
    epc_tag = input.find('p', class_='w9r0350')
    if epc_tag:
        epc_text = epc_tag.get_text(strip=True)
        if 'EPC Rating:' in epc_text:
            data['EPC_Rating'] = epc_text.split(':')[1].strip().replace('E','').replace('B','').replace('A','').replace('C','').replace('D','').strip() + 'D'

    return data


# async def scrape(playwright: Playwright, producer, url=BASE_URL):
async def scrape(playwright: Playwright):
    if not AUTH:
        raise Exception('AUTH environment variable not set. Check your .env file.')

    print('Connecting to Browser...')
    endpoint_url = f'wss://{AUTH}@brd.superproxy.io:9222'
    browser = await playwright.chromium.connect_over_cdp(endpoint_url) 
    try:
        print(f'Connected! Navigating to {url}...')
        page = await browser.new_page()
        await page.goto(url, timeout=2*60_000)

        # Enter London in the search box and press enter
        await page.fill('input[name="autosuggest-input"]', LOCATION)
        await page.keyboard.press('Enter')
        print('Waiting for search results..')

        await page.wait_for_load_state("load")

        # Get the HTML content of the listings container
        content = await page.inner_html('div[data-testid="regular-listings"]')

        soup = BeautifulSoup(content, 'html.parser')

        for idx, div in enumerate(soup.find_all("div", class_="dkr2t86")):
            link = div.find('a')['href'] 
            data = {
                "address": div.find('address').text,
                "link": BASE_URL + link
            }
            
            try:
                # goto the listing page
                print("Navigating to the listing page...", data["link"])
                await page.goto(data["link"], timeout=2*60_000)
                await page.wait_for_load_state("load")

                # extract data from the page
                content = await page.inner_html('div[class="_1olqsf91"]')
                soup = BeautifulSoup(content, 'html.parser')

                # ... โค้ดการเรียกฟังก์ชัน extract_...
                picture_section = soup.find('div', class_='_14l7d4c0')
                pictures = extract_pictures(picture_section)
                data['pictures'] = pictures

                property_details = soup.select_one('div', attrs={'aria-label':'Listing details'}) # div[class="_1olqsf96"]
                property_details = extract_property_details(property_details)

                more_infomation = soup.find('section', attrs={'aria-labelledby': 'key-info'})
                more_infomation = extract_more_infomation(more_infomation)

                data.update(property_details)
                data.update(more_infomation)
                print("Extracting Item #",idx)

                print(data)
                # print("Sending data to kafka")
                # producer.send("properties", value=json.dumps(data).encode('utf-8'))
                # print("Data sent to Kafka")
                # break

            except TimeoutError:
                print(f"Navigation to {data['link']} timed out. Skipping to next listing.")
                continue # ข้ามไปยังรอบต่อไปของลูป

            except Exception as e:
                print(f"An error occurred while processing {data['link']}: {e}. Skipping to next listing.")
                continue

    finally:
        await browser.close()


async def main():
    # producer = KafkaProducer(bootstrap_servers=["kafka-broker:9092"], max_block_ms=10000)
    async with async_playwright() as playwright:
        await scrape(playwright)
        # await scrape(playwright, producer)


if __name__ == '__main__':
    asyncio.run(main())