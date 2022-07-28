import os
import sys
import datetime
import configparser
import pickle
import pandas as pd
from slacker import Slacker
import traceback
import fitbit

import logging
import logging.config
logging.config.fileConfig("../conf/logging.ini")
logger = logging.getLogger(__name__)


class ExFitbit(fitbit.Fitbit):
    
    US = 'en_US'
    
    def __init__(self, client_id, client_secret, access_token=None,
            refresh_token=None, expires_at=None, refresh_cb=None,
            redirect_uri=None, system=US, **kwargs):
        super().__init__(client_id, client_secret, access_token,
                refresh_token, expires_at, refresh_cb,
                redirect_uri, system, **kwargs)
        

    def intraday_time_series_all(self, resource, base_date='today', start_time=None, end_time=None):

        # Check that the time range is valid
        time_test = lambda t: not (t is None or isinstance(t, str) and not t)
        time_map = list(map(time_test, [start_time, end_time]))
        if not all(time_map) and any(time_map):
            raise TypeError('You must provide both the end and start time or neither')

        url = "{0}/{1}/user/-/{resource}/date/{base_date}/all".format(
            *self._get_common_args(),
            resource=resource,
            base_date=self._get_date_string(base_date),
        )

        if all(time_map):
            url = url + '/time'
            for time in [start_time, end_time]:
                time_str = time
                if not isinstance(time_str, str):
                    time_str = time.strftime('%H:%M')
                url = url + ('/%s' % (time_str))

        url = url + '.json'

        return self.make_request(url)


    def get_sleep_by_version(self, date, version=1.2):
        """
        https://dev.fitbit.com/docs/sleep/#get-sleep-logs
        date should be a datetime.date object.
        """
        url = "{0}/{1}/user/-/sleep/date/{year}-{month}-{day}.json".format(
            self._get_common_args()[0],
            version,
            year=date.year,
            month=date.month,
            day=date.day
        )
        return self.make_request(url)


    
def notify_slack(start=True, channel=None, as_user=True):
    
    def _notify_slack(func):         
        def wrapper(*arg, **kwargs):
            if start:
                slack_msg('Start fetch.py', channel=channel, as_user=as_user)
            ret = func(*arg, **kwargs)
            slack_msg('End fetch.py', channel=channel, as_user=as_user)
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


def dump_and_load_dict(d, path):
    
    with open(path, 'wb') as f:
        pickle.dump(d, f)
    with open(path, 'rb') as f:
        return pickle.load(f)
    

def get_dates_for_fetch(dir_path, user_id):

    files = os.listdir(dir_path)
    yyyymmdds = [f.split('_')[-2] for f in files if '.pkl' in f]
    
    if len(yyyymmdds) == 0:
        latest_file_date = None
    else:
        latest_file_date = datetime.datetime.strptime(max(yyyymmdds), '%Y%m%d').strftime('%Y-%m-%d')
    

    df = pd.read_csv('../data/master.csv', index_col='ind')
    cond = (user_id == df['user_id'])
    
    present_dt = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    start_dt = datetime.datetime.strptime(df.loc[cond, 'start_date'].values[0], '%Y-%m-%d')
    if latest_file_date is None:
        dt = start_dt
    else:
        latest_file_dt = datetime.datetime.strptime(latest_file_date, '%Y-%m-%d')
        dt = max(latest_file_dt, start_dt)
    
    dates = []
    while dt < present_dt:
        # latest latest_file_dt = dt = present_dt - 1. So must be more than one files.
        dates.append(dt.strftime('%Y-%m-%d'))
        dt += datetime.timedelta(days=1)

    if len(dates) > 0:
        latest_dt_when_sync_every_day = present_dt - datetime.timedelta(days=2)
        no_sync_days = (latest_dt_when_sync_every_day - datetime.datetime.strptime(dates[0], '%Y-%m-%d')).days
        if no_sync_days >= config.getint('fitbit', 'warn_no_sync_days'):
            msg = f"user_id: {user_id} has to be sync from {dates[0]} of {dir_path.split('/')[-1]}. No sync time is too long."
            logger.warning(msg)
            slack_warning(msg)

    return dates
    

@log_info
def update_token(d_token):
    
    df = pd.read_csv('../data/master.csv', index_col='ind')
    
    cond = (d_token['user_id'] == df['user_id'])
    if sum(cond) != 1:
        raise Exception(f'Cannot update refresh_token: {str(d_token)}')
    
    df.loc[cond, 'access_token'] = d_token['access_token']
    df.loc[cond, 'refresh_token'] = d_token['refresh_token']
    df.loc[cond, 'expires_in'] = d_token['expires_in']
    df.loc[cond, 'updated_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df.loc[cond, 'updated_cnt'] += 1
    
    df.to_csv('../data/master.csv')
    
    logger.info(f"Updated access_token of user_id: {df.loc[cond, 'user_id'].values[0]}, updated_cnt: {df.loc[cond, 'updated_cnt'].values[0]}")
    

def create_initial_dirs(user_id):
    
    if not os.path.exists(f"../data/{user_id}"):

        TARGETS = ['heart-intraday', 'hrv', 'sleep']

        for t in TARGETS:
            os.makedirs(f"../data/{user_id}/1_raw/{t}")
            os.makedirs(f"../data/{user_id}/2_preprocessed/{t}")
        os.makedirs(f"../data/{user_id}/3_processed")
    
        logger.info(f"Initialized dirs of user_id: {user_id}")    


def get_fitbit_client(access_token, refresh_token):
    return ExFitbit(config.get('fitbit', 'client_id'),
                    config.get('fitbit', 'client_secret'),
                    access_token=access_token,
                    refresh_token=refresh_token,
                    refresh_cb=update_token)


def _update_heart_rate(fb, user_id):
    
    logger.info(f"Start update heart-intraday data of user_id: {user_id}")

    dates = get_dates_for_fetch(f"../data/{user_id}/1_raw/heart-intraday", user_id)
    dfs = []
    for date in dates:
        
        response = fb.intraday_time_series('activities/heart', base_date=date, detail_level='1sec')
        d = dump_and_load_dict(response, f"../data/{user_id}/1_raw/heart-intraday/1_raw_heart-intraday_{date.replace('-','')}_{user_id}.pkl")
        logger.info(f"Fetched heart-intraday in {date} of user_id: {user_id}")

        df_raw = pd.DataFrame(d['activities-heart-intraday']['dataset'])
        if len(df_raw) == 0:
            msg = f"Missing heart-intraday in {date} of user_id: {user_id}"
            logger.info(msg)
            slack_msg(msg)
            continue
        df_raw['datetime'] = date + ' ' + df_raw['time']
        df_raw = df_raw[['datetime', 'value']].set_index('datetime')
        df_raw.to_csv(f"../data/{user_id}/2_preprocessed/heart-intraday/2_preprocessed_heart-intraday_{date.replace('-','')}_{user_id}.csv")

    dfs = []
    for file in sorted(os.listdir(f"../data/{user_id}/2_preprocessed/heart-intraday")):        
        dfs.append(pd.read_csv(f"../data/{user_id}/2_preprocessed/heart-intraday/{file}", index_col=0))
    if len(dfs) > 0:
        df = pd.concat(dfs)
        df.to_csv(f"../data/{user_id}/3_processed/3_processed_heart-intraday_{user_id}.csv")
    
    logger.info(f"End update heart-intraday data of user_id: {user_id}")


def _update_hrv(fb, user_id):
    
    logger.info(f"Start update hrv data of user_id: {user_id}")

    dates = get_dates_for_fetch(f"../data/{user_id}/1_raw/hrv", user_id)
    dfs = []
    for date in dates:
        
        response = fb.intraday_time_series_all('hrv', base_date=date)
        d = dump_and_load_dict(response, f"../data/{user_id}/1_raw/hrv/1_raw_hrv_{date.replace('-','')}_{user_id}.pkl")
        logger.info(f"Fetched hrv in {date} of user_id: {user_id}")
        
        hrv_list = d['hrv']
        if len(hrv_list) == 0:
            msg = f"Missing hrv in {date} of user_id: {user_id}"
            logger.info(msg)
            slack_msg(msg)
            continue
        
        df_raws = []
        for d_hrv in hrv_list:
            
            df_ = pd.DataFrame(d_hrv['minutes'])
            df_raw = pd.DataFrame(df_['value'].tolist())
            df_raw['datetime'] = pd.to_datetime(df_['minute'].values)
            df_raw.set_index('datetime', inplace=True)
            df_raws.append(df_raw)
        df_raw = pd.concat(df_raws)
        
        df_raw.to_csv(f"../data/{user_id}/2_preprocessed/hrv/2_preprocessed_hrv_{date.replace('-','')}_{user_id}.csv")

    dfs = []
    for file in sorted(os.listdir(f"../data/{user_id}/2_preprocessed/hrv")):        
        dfs.append(pd.read_csv(f"../data/{user_id}/2_preprocessed/hrv/{file}", index_col=0))
    if len(dfs) > 0:
        df = pd.concat(dfs)
        df.to_csv(f"../data/{user_id}/3_processed/3_processed_hrv_{user_id}.csv")

    logger.info(f"End update hrv data of user_id: {user_id}")


def _update_sleep(fb, user_id):
    
    logger.info(f"Start update sleep data of user_id: {user_id}")
    
    dates = get_dates_for_fetch(f"../data/{user_id}/1_raw/sleep", user_id)
    dfs = []
    for date in dates:
        
        response = fb.get_sleep_by_version(date=datetime.datetime.strptime(date, '%Y-%m-%d'), version=1.2)
        d = dump_and_load_dict(response, f"../data/{user_id}/1_raw/sleep/1_raw_sleep_{date.replace('-','')}_{user_id}.pkl")
        logger.info(f"Fetched sleep in {date} of user_id: {user_id}")
        
        sleep_list = d['sleep']
        if len(sleep_list) == 0:
            msg = f"Missing sleep in {date} of user_id: {user_id}"
            logger.info(msg)
            slack_msg(msg)
            continue
        
        COLS = ['date', 'start_datetime', 'end_datetime', 'log_type', 'type', 'time_in_bed',
                'deep', 'light', 'rem', 'wake', 'asleep', 'awake', 'restless']
        df_raw = pd.DataFrame(columns=COLS)
        for d_sleep in sleep_list[::-1]:
            
            sr = pd.Series(data=0, index=COLS, dtype=object)
            sr['date'] = date
            sr['start_datetime'] = pd.to_datetime(d_sleep['startTime']).strftime('%Y-%m-%d %H:%M:%S')
            sr['end_datetime'] = pd.to_datetime(d_sleep['endTime']).strftime('%Y-%m-%d %H:%M:%S')
            sr['log_type'] = d_sleep['logType']
            sr['type'] = d_sleep['type']
            sr['time_in_bed'] = d_sleep['timeInBed']
            for k, v in d_sleep['levels']['summary'].items():
                sr[k] = v['minutes']
            df_raw.loc[len(df_raw)] = sr
            
        df_raw.to_csv(f"../data/{user_id}/2_preprocessed/sleep/2_preprocessed_sleep_{date.replace('-','')}_{user_id}.csv")
        
    dfs = []
    for file in sorted(os.listdir(f"../data/{user_id}/2_preprocessed/sleep")):        
        dfs.append(pd.read_csv(f"../data/{user_id}/2_preprocessed/sleep/{file}", index_col=0))
    if len(dfs) > 0:
        df_detail = pd.concat(dfs)
        df_detail.sort_values("end_datetime", inplace=True)
        df_detail.reset_index(drop=True, inplace=True)
        df_detail.to_csv(f"../data/{user_id}/3_processed/3_processed_sleep-detail_{user_id}.csv")
        
        df = df_detail.query("log_type == 'auto_detected'").assign(cnt=1)
        df = df[['date', 'cnt', 'time_in_bed', 'deep', 'light', 'rem', 'wake']].groupby('date').sum()
        df.sort_index(inplace=True)
        df.to_csv(f"../data/{user_id}/3_processed/3_processed_sleep_{user_id}.csv")

    logger.info(f"End update hrv data of user_id: {user_id}")


@handle_error(_continue=True)
def update_data(user_id, access_token, refresh_token):
    
    logger.info(f"Start update data of user_id: {user_id}")
    
    # df = pd.read_csv('../data/master.csv', index_col='ind')
    # access_token = df['access_token'].values[0]
    # refresh_token = df['refresh_token'].values[0]
    fb = get_fitbit_client(access_token, refresh_token)
    create_initial_dirs(user_id)
    _update_heart_rate(fb, user_id)
    _update_hrv(fb, user_id)
    _update_sleep(fb, user_id)

    logger.info(f"End update data of user_id: {user_id}")
        

@handle_error(_continue=False)
@notify_slack()
@log_info
def main():
    
    df = pd.read_csv('../data/master.csv', index_col='ind')
    for i, r in df.iterrows():
        
        if r['delete_flg'] == 1 or not (r['start_date'] <= datetime.datetime.today().strftime('%Y-%m-%d') <= r['end_date']):
            logger.info(f"Skip update data of user_id: {r['user_id']}")
            continue
        
        update_data(r['user_id'], r['access_token'], r['refresh_token'])


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('../conf/config.ini')
    slack = Slacker(config.get("slack", "token"))
    main()
    

# response = fb.intraday_time_series_all('hrv', base_date="2022-07-01")
# response = fb.intraday_time_series('activities/heart', base_date=date, detail_level='1sec')
# date = datetime.datetime.today() - datetime.timedelta(days=2)
# response = fb.get_sleep_by_version(date=date, version=1.2)



# fb.user_profile_get()

# fb.client_secret

