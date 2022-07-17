import os
import datetime
import configparser
import pandas as pd
from fitbit.api import FitbitOauth2Client

from flask import Flask, request, redirect, render_template, url_for
app = Flask(__name__)


def oauth2client():
    return FitbitOauth2Client(config.get('fitbit','client_id'), config.get('fitbit', 'client_secret'))

def authorize_token_url():
    scopes = config.get('fitbit', 'scopes').split(',')
    print(scopes)
    url, _ = oauth2client().authorize_token_url(redirect_uri=config.get('fitbit', 'call_back_url'), scope=scopes)
    return url

def fetch_access_token(code):
    response = oauth2client().fetch_access_token(code, config.get('fitbit', 'call_back_url'))
    return response

def dump_master_data(d):
    
    if not os.path.exists('../data/master.csv'):
        df = pd.DataFrame(columns=['ind',
                                   'user_id',
                                   'start_date',
                                   'end_date',
                                   'access_token',
                                   'refresh_token',
                                   'expires_in',
                                   'created_at',
                                   'updated_at',
                                   'updated_cnt',
                                   'delete_flg',
                                   ]).set_index('ind')
        df.to_csv('../data/master.csv')
        
    df = pd.read_csv('../data/master.csv', index_col='ind')
    
    if d['user_id'] in df['user_id'].values:
        cond = (d['user_id'] == df['user_id'])
        df.loc[cond, 'access_token'] = d['access_token']
        df.loc[cond, 'refresh_token'] = d['refresh_token']
        df.loc[cond, 'expires_in'] = d['expires_in']
        df.loc[cond, 'updated_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df.loc[cond, 'updated_cnt'] += 1
        
    else:
        dt_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        date_str = dt_str[:10]
        insert_row = [d['user_id'],
                      date_str,
                      '2099-12-31',
                      d['access_token'],
                      d['refresh_token'],
                      d['expires_in'],
                      dt_str,
                      dt_str,
                      1,
                      0,
                      ]
        df.loc[len(df)+1] = insert_row
        
    df.to_csv('../data/master.csv')        
    

@app.route('/')
def route():
    return render_template('index.html')


@app.route('/register')
def register():
    url = authorize_token_url()
    return redirect(url)


@app.route('/auth/fitbit_oauth2/callback')
def callback():
    code = request.args.get('code', '')
    response = fetch_access_token(code)
    print(response)
    d = {'user_id': response['user_id'],
         'access_token': response['access_token'],
         'refresh_token': response['refresh_token'],
         'expires_in': response['expires_in'],
         }
    dump_master_data(d)
    
    return redirect(url_for('done', user_id=d['user_id']))


@app.route('/done/<string:user_id>')
def done(user_id):    
    return render_template('done.html',
                           user_id=user_id,
                           )


if __name__ == '__main__':
        
    config = configparser.ConfigParser()
    config.read('../conf/config.ini')
    app.run(port=config.getint('flask','port'))


