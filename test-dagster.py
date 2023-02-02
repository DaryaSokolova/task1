import pandas as pd
from dagster import asset, get_dagster_logger
from sklearn.model_selection import train_test_split
from catboost import Pool, CatBoostRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error

@asset
def df_name():
    # assign data of lists.
    data = {'name': ['wikipedia', 'google', 'mail'], 'url': ['https://ru.wikipedia.org/wiki/%D0%AF%D0%BD%D0%B4%D0%B5%D0%BA%D1%81u', 'https://www.google.com/intl/ru_ru/drive/', 'https://pogoda.mail.ru/prognoz/saratov/']}
    # Create DataFrame
    df = pd.DataFrame(data)
    df.to_csv('df_name.csv')
    return df

@asset
def df_result(df_name):
    df_result = df_name
    df_result['domain_of_url'] = df_result['url'].map(lambda x: str(x).partition("://")[2].partition("/")[0])
    df_result.to_csv('df_result.csv')
    return df_result