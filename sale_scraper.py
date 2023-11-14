import csv
import re
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
from multiprocessing import Pool


class EventScraper:
    """Класс для извлечения данных о скидках на игры с сайта xbox-now.com."""
    URL = 'https://www.xbox-now.com/ru/deal-comparison'
    YAML_PATH = 'extract.yaml'

    def __init__(self, filename='sale_data.csv'):
        self.driver = self._init_webdriver()

        self.filename = filename

    @staticmethod
    def _init_webdriver():
        """Инициализирует веб-драйвер Chrome."""
        service = Service(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        return webdriver.Chrome(service=service, options=options)

    def scrape(self, page_number: int = 1) -> str:
        """Извлекает HTML-код страницы с данными о скидках."""
        print(f"Начало обработки страницы {page_number}")
        start_scapepage_time = time.time()
        local_driver = self._init_webdriver()
        page_url = self.URL if page_number == 1 else f"{self.URL}?page={page_number}"
        local_driver.get(page_url)

        # Ожидание загрузки данных AJAX
        wait = WebDriverWait(local_driver, 30)
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'dt[style="white-space: nowrap; margin-left: 1px"]')))

        # Прокрутка страницы до конца
        last_height = local_driver.execute_script("return document.body.scrollHeight")
        while True:
            # Прокрутка вниз до конца страницы
            local_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # Ожидание загрузки страницы
            time.sleep(5)
            # Вычисление новой высоты прокрутки и сравнение со старой высотой
            new_height = local_driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        page_source = local_driver.page_source
        local_driver.quit()
        end_scrapepage_time = time.time()
        print(
            f"Страница {page_number} обработана. Затрачено времени: {end_scrapepage_time - start_scapepage_time:.2f} сек.")
        return page_source

    def extract(self, row_source: str) -> list[dict] | None:
        """Извлекает данные из источника с использованием YAML-шаблона."""
        extractor = selectorlib.Extractor.from_yaml_file(self.YAML_PATH)
        extracted_data = extractor.extract(row_source)
        return extracted_data['games']

    @classmethod
    def extract_last_page_number(cls, row_source: str) -> int | None:
        """Извлекает номер последней страницы с данными о скидках."""
        extractor = selectorlib.Extractor.from_yaml_file(cls.YAML_PATH)
        extracted_data = extractor.extract(row_source)
        last_page_links = extracted_data.get('pages')
        if last_page_links:
            last_page_url = last_page_links[-1]
            page_number = int(last_page_url.split('=')[-1])
            return page_number
        else:
            return None

    @staticmethod
    def clean_country(country: str) -> str | None:
        """Очищает строку страны от лишних символов."""
        if country:
            return re.sub(r'\*\*', '', country)
        return country

    @staticmethod
    def clean_discount(discount: str) -> str | None:
        """Очищает строку скидки, удаляя лишние части."""
        if discount:
            return re.sub(r' до \(.*\)', '', discount)
        return discount

    @staticmethod
    def clean_date(date: str, is_discount_until: bool = False) -> str | None:
        """Очищает строку даты, удаляя время и часовой пояс для даты окончания скидки."""
        if date:
            if is_discount_until:
                date = re.sub(r' \d+:\d+ [A-Z]+', '', date)
            else:
                date = re.sub(r' \(.*\)', '', date)
        return date

    @staticmethod
    def clean_price(price: str, is_usd: bool = False) -> str | None:
        """Преобразует строку цены в числовой формат, удаляя ненужные символы."""
        if price:
            cleaned_price = re.sub(r'[^\d,]', '', price).replace(',', '.')
            if not is_usd:
                return str(int(float(cleaned_price)))
            return cleaned_price
        return price

    def filter_offers(self, games_data: list[dict]) -> list[dict]:
        """Фильтрует предложения по определенным критериям."""
        filtered_data = []
        for game in games_data:
            # Извлечение даты релиза и срока действия скидки
            release_date = self.clean_date(
                next((text.split(': ')[1] for text in game['deal_until'] if "Дата релиза" in text), None))
            discount_until = self.clean_date(
                next((text.split(': ')[1] for text in game['deal_until'] if "Deal until" in text), None),
                is_discount_until=True)

            game_info = {
                'name': game['name'],
                'image': game['image'],
                'release_date': release_date,
                'discount_until': discount_until,
                'USA': None,
                'other_country': None
            }

            for offer in game['offers']:
                country = self.clean_country(offer['country'])
                if country == 'США':
                    if 'on_sale' in offer and 'с GOLD' not in offer['on_sale']:
                        game_info['USA'] = {
                            'type': offer.get('on_sale'),
                            'price_rub': self.clean_price(offer.get('price_rub')),
                            'price_usd': self.clean_price(offer.get('price_usd'), is_usd=True)
                        }
                    elif 'с GOLD' in offer.get('on_sale', ''):
                        game_info['USA'] = {
                            'type': offer['on_sale'],
                            'price_rub': self.clean_price(offer.get('price_discount_rub')),
                            'price_usd': self.clean_price(offer.get('price_discount_usd'), is_usd=True)
                        }

                elif country in ['Аргентина', 'Турция']:
                    if 'on_sale' in offer and 'с GOLD' not in offer['on_sale']:
                        game_info['other_country'] = {
                            'country': country,
                            'type': offer['on_sale'],
                            'price_rub': self.clean_price(offer.get('price_rub')),
                            'price_usd': self.clean_price(offer.get('price_usd'), is_usd=True)
                        }
                    elif 'с GOLD' in offer.get('on_sale', ''):
                        game_info['other_country'] = {
                            'country': country,
                            'type': offer['on_sale'],
                            'price_rub': self.clean_price(offer.get('price_discount_rub')),
                            'price_usd': self.clean_price(offer.get('price_discount_usd'), is_usd=True)
                        }

            if game_info['USA'] and game_info['other_country']:
                filtered_data.append(game_info)

        return filtered_data

    def save_to_csv(self, games_data: list[dict], filename=None) -> str:
        """Сохраняет данные в CSV-файл."""
        filename = filename or self.filename
        with open(filename, 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([
                'id', 'title', 'usa_price', 'usa_price_usd', 'discount_type',
                'other_country', 'other_price_rub', 'other_price_usd', 'discount_other_type',
                'discount_until', 'release_date', 'image'
            ])

            for number, game in enumerate(games_data):
                usa_info = game['USA']
                other_info = game['other_country']
                writer.writerow([
                    number + 1,
                    game['name'],
                    usa_info.get('price_rub', ''),
                    usa_info.get('price_usd', ''),
                    usa_info.get('type', ''),
                    other_info.get('country', ''),
                    other_info.get('price_rub', ''),
                    other_info.get('price_usd', ''),
                    other_info.get('type', ''),
                    game.get('discount_until', ''),
                    game.get('release_date', ''),
                    game['image']
                ])
        return filename

    def close(self):
        self.driver.quit()


def scrape_page(page_number):
    local_scraper = EventScraper()
    try:
        return local_scraper.scrape(page_number)
    finally:
        local_scraper.close()


if __name__ == '__main__':
    scraper = EventScraper()
    start_time = time.time()
    try:
        # Сначала получаем номер последней страницы
        source = scraper.scrape()
        last_page_number = scraper.extract_last_page_number(source)

        print(f"Всего страниц для скрапинга: {last_page_number}")

        # Затем запускаем многопроцессный скрапинг
        with Pool(processes=os.cpu_count()) as pool:
            results = pool.map(scrape_page, range(1, last_page_number + 1))
            # results = pool.map(scrape_page, [1])
        all_games_data = []
        for result in results:
            data = scraper.extract(result)
            all_games_data.extend(data)
        # pprint(all_games_data)
        filtered_games_data = scraper.filter_offers(all_games_data)
        scraper.save_to_csv(filtered_games_data)
    finally:
        scraper.close()
        end_time = time.time()
        print(f"Скрапинг завершен. Общее время: {end_time - start_time:.2f} сек.")
