import requests
import pandas as pd
import os

from bs4 import BeautifulSoup
from pathlib import Path

page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
os.mkdir('./data')