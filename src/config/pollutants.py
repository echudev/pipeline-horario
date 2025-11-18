"""
Configuration for pollutant tables and metrics
"""
from typing import Dict, List

TABLE_CONFIG: Dict[str,dict] = {
    'co': {
        'table': 'co_minutales',
        'metrics': ['co_mean']
    },
    'nox': {
        'table': 'nox_minutales',
        'metrics': ['no_mean', 'no2_mean', 'nox_mean']
    },
    'pm10': {
        'table': 'pm10_minutales',
        'metrics': ['pm10_mean']
    },
    'so2': {
        'table': 'so2_minutales',
        'metrics': ['so2_mean']
    },
    'o3': {
        'table': 'o3_minutales',
        'metrics': ['o3_mean']
    },
    'meteo': {
        'table': 'meteo_minutales',
        'metrics': ['*']  # All columns
    }
}

# Pollutants to process in the pipeline (excludes 'meteo')
POLLUTANTS_TO_PROCESS: List[str] = [
    key for key in TABLE_CONFIG.keys() if key != 'meteo'
]

