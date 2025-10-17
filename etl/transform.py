import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

def date_mod(x):
    x = str(x)
    x = x.replace(' 0:00','')
    return pd.to_datetime(x, format='%m/%d/%Y')

def transform_data(df):
    logger.info("Transforming data...")
    try:
        df.drop(columns=['STATE','POSTALCODE','TERRITORY','ADDRESSLINE2'], inplace=True, errors='ignore')
        df.columns = ['order_id', 'order_quantity', 'price', 'line_item', 'sales', 'order_date',
                      'order_status', 'order_quarter', 'order_month', 'order_year', 'product_line',
                      'msrp', 'product_id', 'customer_name', 'phone', 'address', 'city', 'country',
                      'last_name', 'first_name', 'deal_size']
        
        df['order_date'] = df['order_date'].replace(' 0:00','')
        df['order_date'] = df['order_date'].apply(date_mod)
        logger.info("Data transformation complete.")
        return df
    
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise