from abc import ABC, abstractmethod
import os
import json
import warnings
import pandas as pd
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm
from tqdm import tqdm
from config import *
import logging
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M' #datefmt='%Y-%m-%d %H:%M:%S'
)


'''CALCULATOR'''
class signal_calculator(ABC):
    
    def __init__(self, input_market_data):
        self.input_market_data = input_market_data
        
    @abstractmethod
    def calculate_data(self):
        pass
    
    @abstractmethod
    def calculate_signal(self, output_df):
        pass


