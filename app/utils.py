import sys
import pandas as pd
import numpy as np
from urllib.parse import urlparse,parse_qs

from logger import get_log

LOG = get_log(__name__)

def get_data(path: str) -> pd.DataFrame:
    """Read data.
    Given a data path, return the dataframe.
    Args:
        path: data path
    Returns:
        The raw dataframe is returned.
    """
    try:
        raw_df = pd.read_csv(path,sep="\t")
        LOG.info(f"data: retrieved [{raw_df.shape[0]}] records")
    except Exception as error:
        LOG.exception(f"data: source data could not be loaded. {error}")
        sys.exit(1)

    if raw_df.shape[0] == 0:
        LOG.exception(f"data: source data empty.")
        sys.exit(1)

    return raw_df

def deduplicate_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate data.
    Given a dataframe, return the dedup dataframe.
    Args:
        path: dataframe
    Returns:
        The dedup dataframe is returned.
    """
    try:
        dedup_df = raw_df.drop_duplicates()
        LOG.info(f"data: deduplicate [{len(raw_df) - len(dedup_df)}] records")
    except Exception as error:
        LOG.exception(f"data: deduplicate could not be completed. {error}")
        sys.exit(1)
    return dedup_df

def generate_metric(dedup_df: pd.DataFrame) -> pd.DataFrame:
    """Generate metric.
    Given a dataframe, return the metric dataframe.
    Args:
        path: dataframe
    Returns:
        The metric dataframe is returned.
    """
    try:
        # Create Array for Product List
        dedup_df['product_list_array'] = dedup_df['product_list'].str.split(',')

        # Create separate row for each Product
        df_explode = dedup_df.explode('product_list_array')

        # Create columns for Each Product Attributes
        df_explode[['Category','Product_Name','Number_of_Items','Total_Revenue','Custom_Event']] = df_explode['product_list_array'].str.split(';',4,expand=True)

        #Parse the URL to get Domain, and Search Keywords
        df_explode['page_domain'] = df_explode['page_url'].apply(lambda x:urlparse(x).netloc)
        df_explode['referrer_domain'] = df_explode['referrer'].apply(lambda x:urlparse(x).netloc)
        df_explode['referrer_path'] = df_explode['referrer'].apply(lambda x:urlparse(x).path)
        df_explode['referrer_params'] = df_explode['referrer'].apply(lambda x:urlparse(x).params)
        df_explode['referrer_query'] = df_explode['referrer'].apply(lambda x: urlparse(x).query.lower())
        df_explode['search_key_word'] = df_explode['referrer_query'].apply(lambda x: parse_qs(x).get('q',''))
        df_explode['date'] = df_explode['date_time'].str.split(' ',1,expand=True)[0]
        df_explode['search_key_word_str'] = df_explode['search_key_word'].apply(lambda x:",".join(x))

        # Ranke the web browsing steps from Search to Purchase
        df_explode['rank'] = df_explode.sort_values(['hit_time_gmt'], ascending=True)\
             .groupby(['date','user_agent','ip'])\
             .cumcount() + 1

        # Filter data for Entry point and purchase event (1st and Last Step)
        df_filter = df_explode[(df_explode['rank'] == 1) | (df_explode['event_list'] == 1.0) ]

        # Windows functions to get entry point and search key word in purchase event row  - LAG window functions
        df_filter['Search_Engine_Domain'] = df_filter.sort_values(by=['hit_time_gmt'], ascending=True)\
                       .groupby(['date','user_agent','ip'])['referrer_domain'].shift(1)
        df_filter['entry_search_keyword'] = df_filter.sort_values(by=['hit_time_gmt'], ascending=True)\
                               .groupby(['date','user_agent','ip'])['search_key_word'].shift(1)
        df_filter['Search_Keyword'] = df_filter.sort_values(by=['hit_time_gmt'], ascending=True)\
                               .groupby(['date','user_agent','ip'])['search_key_word_str'].shift(1)

        # Clean the data drop null values from Total Total_Revenue
        df_clean_revenue = df_filter.dropna(subset=['Total_Revenue','Search_Engine_Domain'])
        df_clean_revenue['Total_Revenue'] = df_clean_revenue['Total_Revenue'].astype(float)

        # Aggregate Total_Revenue by Domain, Search Keyword
        df_final = df_clean_revenue.groupby(['date','Search_Engine_Domain','Search_Keyword'])['Total_Revenue'].sum().to_frame()\
           .sort_values(by=['Total_Revenue'], ascending=False)

        metric_df = pd.DataFrame(df_final).reset_index()

        LOG.info(f"data: generate_metric [{metric_df.shape[0]}] records")
    except Exception as error:
        LOG.exception(f"data: generate_metric could not be completed. {error}")
    return metric_df


def print_demo(s: str):
    print(s)
