import numpy as np
import pandas as pd
import env
import os

#connection set up
def get_connection(db, user=env.user, host=env.host, password=env.password):
    return f'mysql+pymysql://{env.user}:{env.password}@{env.host}/{db}'

def get_logs():
#pull df locally or use sql query
    if os.path.isfile('curriculum_logs.csv'):
        return pd.read_csv('curriculum_logs.csv', index_col=0)

    else:
        ''' Pulls curriculum log from CodeUP database and translate it to dataframe'''
        query = '''
        SELECT logs.date,  logs.time,
        logs.path as endpoint,
        logs.user_id,
        logs.ip,
        cohorts.name as cohort_name,
        cohorts.start_date,
        cohorts.end_date,
        cohorts.program_id
        FROM logs
        JOIN cohorts ON logs.cohort_id= cohorts.id;
        '''
        
        
        df= pd.read_sql(query, get_connection('curriculum_logs'))
        df.to_csv('curriculum_logs.csv')
    return df


def prepare_log():
    ''' This prepare function set the date column as index, drop unwanted columns    and set the start date and end date to date time format'''
    df = get_logs()
    # Reassign the sale_date column to be a datetime type
    df.date = pd.to_datetime(df.date)
    # Sort rows by the date and then set the index as that date
    df = df.set_index("date").sort_index()
    #set the start_date and end_date column to datetime format
    df.start_date = pd.to_datetime(df.start_date)
    df.end_date = pd.to_datetime(df.end_date)
 
    return df



def prep(df, user):
    """ Prepare a df with records only from specific user """
    # Make a df with records only from specific user
    user_df = df[df.user_id == user]
    # Resample user_df by day
    pages = user_df.endpoint.resample('d').count()
    return pages

def compute_pct_b(pages, span, weight, user):
    """ Compute the %b (Bollinger Band percentage) and output results as df """
    midband = pages.ewm(span=span).mean()
    stdev = pages.ewm(span=span).std()
    ub = midband + stdev*weight
    lb = midband - stdev*weight
    bb = pd.concat([ub, lb], axis=1)
    my_df = pd.concat([pages, midband, bb], axis=1)
    my_df.columns = ['pages', 'midband', 'ub', 'lb']
    my_df['pct_b'] = (my_df['pages'] - my_df['lb'])/(my_df['ub'] - my_df['lb'])
    my_df['user_id'] = user
    return my_df

def plt_bands(my_df, user):
    """ Plot pages with Bollinger Bands """
    fig, ax = plt.subplots(figsize=(12,8))
    ax.plot(my_df.index, my_df.pages, label='Number of Pages, User: '+str(user))
    ax.plot(my_df.index, my_df.midband, label = 'EMA/midband')
    ax.plot(my_df.index, my_df.ub, label = 'Upper Band')
    ax.plot(my_df.index, my_df.lb, label = 'Lower Band')
    ax.legend(loc='best')
    ax.set_ylabel('Number of Pages')
    plt.show()
    
    
def find_anomalies(df, user, span, weight):
    """ Identify anomalies, meaning %b is above 1. This equates to > weight*std above the EMA with length span"""
    pages = prep(df, user)
    my_df = compute_pct_b(pages, span, weight, user)
    # plt_bands(my_df, user)
    return my_df[my_df.pct_b>1]