import os
import sys
import datetime
import configparser
import pandas as pd
from slacker import Slacker
import traceback
import fitbit

import logging
import logging.config
logging.config.fileConfig("../conf/logging.ini")
logger = logging.getLogger(__name__)


def notify_slack(start=True, channel=None, as_user=True):
    
    def _notify_slack(func):         
        def wrapper(*arg, **kwargs):
            if start:
                slack_msg(f'Start fetch.py', channel=channel, as_user=as_user)
            ret = func(*arg, **kwargs)
            slack_msg(f'End fetch.py', channel=channel, as_user=as_user)
            return ret
        return wrapper
    return _notify_slack


def slack_msg(msg, channel=None, as_user=True):
    
    if channel is None:
        channel = config.get("slack", "channel")
        
    slack.chat.post_message(channel, msg, as_user=as_user)


def slack_error(msg, channel=None, as_user=True):
    
    msg = f"<!channel> ERROR: {msg}"
    slack_msg(msg, channel, as_user)


def slack_warning(msg, channel=None, as_user=True):
    
    msg = f"<!here> WARNING: {msg}"
    slack_msg(msg, channel, as_user)


def handle_error(_continue=True):    
    
    def _handle_error(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.exception(e)
                slack_error(traceback.format_exc())
                if _continue:
                    logger.info("Continue process")
                else:
                    sys.exit(1)
        return wrapper
    return _handle_error


def log_info(func):    

    def wrapper(*args, **kwargs):
        logger.info(f"Start {func.__name__}")
        ret = func(*args, **kwargs)
        logger.info(f"End {func.__name__}")
        return ret

    return wrapper


def get_dates_for_fetch(dir_path, user_id):

    files = os.listdir(dir_path)
    yyyymmdds = [f.split('_')[-2] for f in files if '.csv' in f]
    
    if len(yyyymmdds) == 0:
        latest_file_date = None
    else:
        latest_file_date = datetime.datetime.strptime(max(yyyymmdds), '%Y%m%d').strftime('%Y-%m-%d')
    

    df = pd.read_csv('../data/master.csv', index_col='ind')
    cond = (user_id == df['user_id'])
    
    present_dt = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    fetch_start_dt = datetime.datetime.strptime(df.loc[cond, 'fetch_start_date'].values[0], '%Y-%m-%d')
    if latest_file_date is None:
        dt = fetch_start_dt
    else:
        latest_file_dt = datetime.datetime.strptime(latest_file_date, '%Y-%m-%d')
        dt = max(latest_file_dt, fetch_start_dt)
    
    dates = []
    while dt < present_dt:
        # latest latest_file_dt = dt = present_dt - 1. So must be more than one files.
        dates.append(dt.strftime('%Y-%m-%d'))
        dt += datetime.timedelta(days=1)

    latest_dt_when_sync_every_day = present_dt - datetime.timedelta(days=2)
    no_sync_days = (latest_dt_when_sync_every_day - datetime.datetime.strptime(dates[0], '%Y-%m-%d')).days
    if no_sync_days >= config.getint('fitbit', 'warn_no_sync_days'):
        msg = f"user_id: {user_id} has to be sync from {dates[0]} of {dir_path.split('/')[-1]}. No sync time is too long."
        logger.warn(msg)
        slack_warning(msg)

    return dates
    

@log_info
def update_token(d_token):
    
    df = pd.read_csv('../data/master.csv', index_col='ind')
    
    cond = (d_token['user_id'] == df['user_id'])
    if sum(cond) != 1:
        raise Exception(f'Cannot update refresh_token: {str(d_token)}')
    
    df.loc[cond, 'access_token'] = d_token['access_token']
    df.loc[cond, 'expires_in'] = d_token['expires_in']
    df.loc[cond, 'updated_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df.loc[cond, 'updated_cnt'] += 1
    
    df.to_csv('../data/master.csv')
    
    logger.info(f"Updated access_token of user_id: {df.loc[cond, 'user_id'].values[0]}, updated_cnt: {df.loc[cond, 'updated_cnt'].values[0]}")
    

def create_initial_dirs(user_id):
    
    if not os.path.exists(f"../data/{user_id}"):
        os.makedirs(f"../data/{user_id}/1_raw/heart-intraday")
        os.makedirs(f"../data/{user_id}/2_preprocessed")
        os.makedirs(f"../data/{user_id}/3_processed")
    
    logger.info(f"Initialized dirs of user_id: {user_id}")    


def get_fitbit_client(access_token, refresh_token):
    return fitbit.Fitbit(config.get('fitbit', 'client_id'),
                         config.get('fitbit', 'client_secret'),
                         access_token=access_token,
                         refresh_token=refresh_token,
                         refresh_cb=update_token)


def _update_heart_rate(fb, user_id):
    
    logger.info(f"Start update heart-intraday data of user_id: {user_id}")
    
    if not os.path.exists(f"../data/{user_id}/2_preprocessed/heart-intraday.csv"):
        df_total = pd.DataFrame(columns=['datetime','value']).set_index('datetime')
    else:
        df_total = pd.read_csv(f"../data/{user_id}/2_preprocessed/heart-intraday.csv", index_col='datetime')
    
    dates = get_dates_for_fetch(f"../data/{user_id}/1_raw/heart-intraday", user_id)
    for date in dates:
        
        response = fb.intraday_time_series('activities/heart', base_date=date, detail_level='1sec')
        logger.info(f"Fetched heart-intraday in {date} of user_id: {user_id}")
        df = pd.DataFrame(response['activities-heart-intraday']['dataset'])
        if len(df) == 0:
            msg = f"Missing heart-intraday in {date} of user_id: {user_id}"
            logger.warn(msg)
            slack_warning(msg)
            continue
        df.to_csv(f"../data/{user_id}/1_raw/heart-intraday/1_raw_heart-intraday_{date.replace('-','')}_{user_id}.csv")
        
        df['datetime'] = date + ' ' + df['time']
        df = df[['datetime','value']].set_index('datetime')
        df_total = df_total[df_total.index.str[:10] != date]
        df_total = df_total.append(df).sort_index()
    
    df_total.to_csv(f"../data/{user_id}/2_preprocessed/heart-intraday.csv")
        
    logger.info(f"End update heart-intraday data of user_id: {user_id}")
    

@handle_error(_continue=True)
def update_data(user_id, access_token, refresh_token):
    
    logger.info(f"Start update data of user_id: {user_id}")
    
    # df = pd.read_csv('../data/master.csv', index_col='ind')
    # access_token = df['access_token'].values[0]
    # refresh_token = df['refresh_token'].values[0]
    fb = get_fitbit_client(access_token, refresh_token)
    create_initial_dirs(user_id)
    _update_heart_rate(fb, user_id)
    
    logger.info(f"End update data of user_id: {user_id}")
        

@handle_error(_continue=False)
@notify_slack()
@log_info
def main():
    
    df = pd.read_csv('../data/master.csv', index_col='ind')
    for i, r in df.iterrows():
        
        if r['delete_flg'] == 1 or not (r['fetch_start_date'] <= datetime.datetime.today().strftime('%Y-%m-%d') <= r['fetch_end_date']):
            logger.info(f"Skip update data of user_id: {r['user_id']}")
            continue
        
        update_data(r['user_id'], r['access_token'], r['refresh_token'])


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('../conf/config.ini')
    slack = Slacker(config.get("slack", "token"))
    main()
    
    
response = fb.intraday_time_series('activities/heart', base_date=date, detail_level='1sec')
response = fb.intraday_time_series('activities/heart', base_date=date, detail_level='1sec')




