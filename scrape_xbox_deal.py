import csv
import re
from multiprocessing import Pool
import os
from pprint import pprint

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import selectorlib
import time

URL = 'https://www.xbox-now.com/ru/deal-comparison'
YAML_PATH = 'extract.yaml'


class Event:
    """Класс для скрапинга данных с помощью Selenium"""

    @staticmethod
    def scrape(url):
        """Метод для скрапинга данных с помощью Selenium"""
        service = Service(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(url)

        # Ожидание загрузки данных AJAX
        wait = WebDriverWait(driver, 30)
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'dt[style="white-space: nowrap; margin-left: 1px"]'))
        )
        # Прокрутка страницы до конца
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            # Прокрутка вниз до конца страницы
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # Ожидание загрузки страницы
            time.sleep(5)
            # Вычисление новой высоты прокрутки и сравнение со старой высотой
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        source = driver.page_source
        driver.quit()
        return source

    @staticmethod
    def extract(source):
        extractor = selectorlib.Extractor.from_yaml_file(YAML_PATH)
        data = extractor.extract(source)
        return data['games']

    @classmethod
    def extract_last_page_number(cls, source):
        extractor = selectorlib.Extractor.from_yaml_file(YAML_PATH)
        data = extractor.extract(source)
        last_page_links = data.get('pages')
        if last_page_links:
            last_page_url = last_page_links[-1]
            page_number = int(last_page_url.split('=')[-1])
            return page_number
        else:
            return None


def scrape_page(page_number):
    print(f"Процесс {os.getpid()} начал скрапинг страницы {page_number}")
    page_url = f"{URL}?page={page_number}"
    event = Event()  # Создаем новый экземпляр Event для каждого процесса
    try:
        source = event.scrape(page_url)
        data = event.extract(source)
        print(f"Процесс {os.getpid()} завершил скрапинг страницы {page_number}")
        return data
    except Exception as e:
        print(f"Ошибка при скрапинге страницы {page_number}: {e}")
        return None


def clean_country(country):
    if country:
        return re.sub(r'\*\*', '', country)
    return country


def clean_discount(discount):
    if discount:
        return re.sub(r' до \(.*\)', '', discount)
    return discount


def clean_date(date, is_discount_until=False):
    if date:
        if is_discount_until:
            # Удаляем время и часовой пояс для даты окончания скидки
            date = re.sub(r' \d+:\d+ [A-Z]+', '', date)
        else:
            # Удаляем все после даты для даты релиза
            date = re.sub(r' \(.*\)', '', date)
    return date


def clean_price(price, is_usd=False):
    if price:
        cleaned_price = re.sub(r'[^\d,]', '', price).replace(',', '.')
        if not is_usd:
            # Если это не USD, округляем до целого числа
            return str(int(float(cleaned_price)))
        return cleaned_price
    return price


def filter_offers(games_data):
    filtered_data = []
    for game in games_data:
        # Очистка даты релиза и срока действия сделки
        release_date = clean_date(
            next((text.split(': ')[1] for text in game['deal_until'] if "Дата релиза" in text), None))
        deal_until = clean_date(
            next((text.split(': ')[1] for text in game['deal_until'] if "Deal until" in text), None),
            is_discount_until=True)

        # Извлечение информации о цене в США
        price_usa = clean_price(
            next((offer['price'] for offer in game['offers'] if clean_country(offer['country']) == 'США'), None))
        price_usd_usa = next(
            (offer['price_usd'] for offer in game['offers'] if clean_country(offer['country']) == 'США'), None)

        # Обработка каждого предложения
        for offer in game['offers']:
            country = clean_country(offer['country'])
            if country and any(c in country for c in ['Аргентина', 'Турция']):
                filtered_data.append({
                    'name': game['name'],
                    'country': country,
                    'price': clean_price(offer['price']),
                    'price_usa': price_usa,
                    'price_usa_in_usd': clean_price(price_usd_usa, is_usd=True),
                    'discount': clean_discount(offer['discount']),
                    'discount_until': deal_until,
                    'release_date': release_date,
                    'image': game['image']
                })
    return filtered_data


if __name__ == "__main__":
    event = Event()
    source = event.scrape(URL)
    last_page_number = Event.extract_last_page_number(source)
    # Создаем пул процессов
    with Pool(processes=os.cpu_count()) as pool:
        # Запускаем скрапинг всех страниц в пуле процессов
        results = pool.map(scrape_page, range(1, last_page_number + 1))
        # results = pool.map(scrape_page, range(1))

    # Объединяем результаты
    all_games_data = [game for result in results if result for game in result]
    filtered_games_data = filter_offers(all_games_data)
    with open('finished_data.csv', 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(
            ['id', 'title', 'country', 'price', 'price_usa', 'price_usa_in_usd', 'discount', 'discount_until',
             'release_date', 'image'])

        for number, game in enumerate(filtered_games_data):
            writer.writerow([
                number + 1,
                game['name'],
                game['country'],
                game['price'],
                game['price_usa'],
                game['price_usa_in_usd'],
                game['discount'],
                game['discount_until'],
                game['release_date'],
                game['image']
            ])
