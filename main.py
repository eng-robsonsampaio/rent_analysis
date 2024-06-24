import queue
import os
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver import ChromeOptions
from time import sleep
import pandas as pd
import requests
import random
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
import matplotlib.pyplot as plt
from pymongo import MongoClient
from dotenv import load_dotenv
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

pd.options.display.max_colwidth = None

load_dotenv(dotenv_path='CREDENTIALS.env')

# Obter credenciais do MongoDB das variáveis de ambiente
mongo_user = os.getenv("MONGO_USER")
mongo_pass = os.getenv("MONGO_PASS")
mongo_cluster = os.getenv("MONGO_CLUSTER")

try:
    # Conecte-se ao MongoDB
    mongo_uri = f"mongodb+srv://{mongo_user}:{mongo_pass}@{mongo_cluster}.0tyg61m.mongodb.net/?retryWrites=true&w=majority"
    print(f"{mongo_uri}\n")
    client = MongoClient(mongo_uri)

    # Selecionar o banco de dados e a coleção
    db = client.production
    collection = db.realEstate

except Exception as e:
    print(f"Falha ao conectar ao MongoDB: {e}")

def extract_bathrooms(text):
    # Procurar por padrões numéricos na string
    matches = re.findall(r'\d+', text)
    if matches:
        return int(matches[0])  # Retorna o primeiro número encontrado como inteiro
    else:
        return None  # Caso não encontre nenhum número, retorna None

def convert_to_numeric(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return None

def extract_listing_data(url):
    options = ChromeOptions()
    
    # Inicializa o WebDriver do Chrome
    driver = webdriver.Chrome(options=options)
    
    # Acessa a página da listagem de imóveis
    driver.get(url)
    sleep(random.randint(2,5))  # Aguarda o carregamento da página (ajuste conforme necessário)
    
    # Obtém o conteúdo da página
    page_source = driver.page_source
    
    # Fecha o WebDriver do Chrome
    driver.quit()

    # Analisa o conteúdo da página com BeautifulSoup
    soup = BeautifulSoup(page_source, 'html.parser')  
    
    # Extrai os dados da página
    try:
        title = soup.find('h1', class_='title__title js-title-view')\
            .get_text(strip=True)
    except AttributeError:
        title = None
    
    try:
        address = soup.find('p', class_='title__address js-address')\
            .get_text(strip=True)
    except AttributeError:
        address = None
    
    try:
        area = soup.find('li', class_='features__item features__item--area js-area')\
            .get_text(strip=True)\
            .replace('m²', '')\
            .strip()
        area = convert_to_numeric(area)
    except AttributeError:
        area = None
    
    try:
        bedrooms = soup.find('li', class_='features__item features__item--bedroom js-bedrooms')\
            .get_text(strip=True)\
            .strip()\
            .replace('quartos', '')\
            .replace('quarto', '')\
            .replace("Não informadoSolicitar", '')\
            .strip()
        bedrooms = convert_to_numeric(bedrooms)
    except AttributeError:
        bedrooms = None
    
    try:
        bathrooms = soup.find('li', class_='features__item features__item--bathroom js-bathrooms')\
            .get_text(strip=True)\
            .strip() 
        bathrooms = extract_bathrooms(bathrooms)       
    except AttributeError:
        bathrooms = None
    
    try:
        parking = soup.find('li', class_='features__item features__item--parking js-parking')\
            .get_text(strip=True)\
            .strip()\
            .replace('vagas', '')\
            .replace('vaga', '')\
            .strip()
        parking = convert_to_numeric(parking)
    except AttributeError:
        parking = None
    
    try:
        price = soup.find('h3', class_='price__price-info js-price-sale')\
            .get_text(strip=True)\
            .strip()\
            .replace('R$', '')\
            .replace('/Mês', '')\
            .replace('.', '')\
            .replace(',', '.')\
            .strip()
        price = convert_to_numeric(price)
    except AttributeError:
        price = None
    
    try:
        condominium = soup.find('span', class_='price__list-value condominium js-condominium')\
            .get_text(strip=True)\
            .strip()\
            .replace('R$', '')\
            .replace('.', '')\
            .replace(',', '.')\
            .strip()
        condominium = convert_to_numeric(condominium)
    except AttributeError:
        condominium = None
    
    try:
        total_rent = soup.find('span', class_='price__list-value rent-condominium js-total-rental-price')\
            .get_text(strip=True)\
            .strip()\
            .replace('R$', '')\
            .replace('.', '')\
            .replace(',', '.')\
            .strip()
        total_rent = convert_to_numeric(total_rent)
    except AttributeError:
        total_rent = None
    
    try:
        iptu = soup.find('span', class_='price__list-value iptu js-iptu')\
            .get_text(strip=True)\
            .strip()\
            .replace('R$', '')\
            .replace('.', '')\
            .replace(',', '.')\
            .strip()
        iptu = convert_to_numeric(iptu)
    except AttributeError:
        iptu = None

    def get_neighborhood(address: str):
        if(len(address.split(',')) > 2):
            return address.split('-')[1].split(',')[0].strip()
        else:
            return address.split(',')[0].strip()
    
    def get_city(address: str):
        return address.split(',')[-1].strip()
    
    def geocode_address(address):
        geolocator = Nominatim(user_agent="geoapiExercises")
        try:
            location = geolocator.geocode(address)
            if location:
                return location.latitude, location.longitude
            else:
                return None, None
        except:
            return None, None
        
    (latitude, longitude) = geocode_address(address)
    
    return {
        "title": title,
        "address": address,
        "neighborhood": get_neighborhood(address),
        "city": get_city(address),
        "latitude": latitude,
        "longitude": longitude,
        "area": area,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "parking": parking,
        "price/mouth": price,
        "condominium": condominium,
        "total_rent": total_rent,
        "iptu": iptu,
        "url": url,
    }

def get_html(url):
    chrome_options = Options()
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    sleep(3)
    html = driver.page_source
    driver.quit()
    soup = BeautifulSoup(html, 'html.parser')
    return soup

def extraction(urls, root_url: str, processed_urls):
    data = []
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_url = {}
        for url in urls:
            full_url = root_url + url
            if full_url not in processed_urls and not collection.find_one({"url": full_url}):
                future_to_url[executor.submit(extract_listing_data, full_url)] = full_url
                processed_urls.add(full_url)
        
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                rent_data = future.result()
                if rent_data:
                    data.append(rent_data)
                    # Inserir dados no MongoDB
                    try:
                        collection.insert_one(rent_data)
                        print(f"\nDados inseridos com sucesso para o URL: {url}\n")
                    except Exception as e:
                        print(f"\nFalha ao inserir dados no MongoDB para o URL {url}: {e}\n")
            except Exception as e:
                print(f"\nErro ao processar {url}: {e}\n")
        
    return data

root_url = "https://www.vivareal.com.br/"
url_text = '?pagina='
url_page = 1
first_page = False
next_page = True
all_data = []
q = queue.Queue()
processed_urls = set()

while next_page:
    if first_page:
        # url = f"{root_url}/aluguel/ceara/fortaleza/#onde=Brasil,Cear%C3%A1,Fortaleza,,,,,,BR%3ECeara%3ENULL%3EFortaleza,,,"
        # url = f"{root_url}/aluguel/ceara/maracanau/#onde=Brasil,Ceará,Maracanaú,,,,,,BR>Ceara>NULL>Maracanau,,,"
        # url = f"{root_url}/aluguel/ceara/eusebio/#onde=Brasil,Ceará,Eusébio,,,,,,BR>Ceara>NULL>Eusebio,,,&tipos=apartamento_residencial,casa_residencial,condominio_residencial,cobertura_residencial,kitnet_residencial,flat_residencial,sobrado_residencial"
        url = f"{root_url}/aluguel/ceara/caucaia/#onde=Brasil,Ceará,Caucaia,,,,,,BR>Ceara>NULL>Caucaia,,,&tipos=apartamento_residencial,casa_residencial,cobertura_residencial,flat_residencial,kitnet_residencial,condominio_residencial"

        first_page = False
    else:        
        url = f"https://www.vivareal.com.br/aluguel/ceara/caucaia/?pagina={url_page}#onde=Brasil,Ceará,Caucaia,,,,,,BR>Ceara>NULL>Caucaia,,,&tipos=apartamento_residencial,casa_residencial,cobertura_residencial,flat_residencial,kitnet_residencial,condominio_residencial"
        print(f"{url}")

    # Obter a resposta da URL inicial
    html = get_html(url)
    cards = html.find('div', {'class': 'results-list js-results-list'})\
        .find_all('a', {'class':'property-card__content-link js-card-title'})
    
    urls = [card['href'] for card in cards]

    page_data = extraction(urls, root_url, processed_urls)
    all_data.extend(page_data)

    if url_page >= 5:
        next_page = False
        print("Última página alcançada.")
        break

    url_page = url_page + 1

# Criar DataFrame com os dados coletados
# df = pd.DataFrame(all_data)
# print(df)
