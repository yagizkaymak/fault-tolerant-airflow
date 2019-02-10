"""
This is the webapp that periodically checks
the status of the active and backup schedulers
and visualizes the average stock prices
as the outcome of the created data pipeline.

This app uses Dash by Plotly and runs on a Flask server.

This application is developed as an Insight data science project.

Author: Yagiz Kaymak
Date: February, 2019
 """

import os
import subprocess
import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
import flask
import pandas as pd
import psycopg2
import datetime

# Timestamp for date to string conversion
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

# Status refresh rate for active and backup schedulers in msec
page_refresh_period = 1000


button_style = {'background-color': '#f44336',
  'border': 'None',
  'color': 'white',
  'padding': '12px 24px',
  'text-align': 'center',
  'text-decoration': 'None',
  'display': 'inline-block',
  'font-size': '12px',
  }

time_style = {'text-align': 'right'}


def connect_db(db_name):
    """ Database connection fucntion """
    try:
    	# connect to the PostgreSQL database
        conn = psycopg2.connect(
    	    host=os.environ.get('PG_HOST'),
    	    database=db_name,
    	    user=os.environ.get('PG_USER'),
    	    password=os.environ.get('PG_PSWD')
    		                   )
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
    	print error

def get_distinct_stocks():
    """ Function that fetches all distinct stocks from DB """
    try:
        conn = connect_db('pds_db')
        cursor = conn.cursor()
        cursor.execute("""SELECT distinct "Ticker" FROM avg_xetr""")
        stock_rows = cursor.fetchall()
        stocks = [i[0] for i in stock_rows]
        cursor.close()
        conn.close()
        return stocks
    except (Exception, psycopg2.DatabaseError) as error:
    	print error

def get_ft_airflow_status():
    """ Fetches the active and backup schedulers, and the time
    of the last heartbeat sent by the active backup scheduler """
    try:
        conn = connect_db('airflow')
        cursor = conn.cursor()
        cursor.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('active_scheduler', ))
        active_scheduler_row = cursor.fetchall()
        active_scheduler = str(active_scheduler_row[0][0])

        cursor.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('active_backup_scheduler', ))
        active_backup_scheduler_row = cursor.fetchall()
        active_backup_scheduler = str(active_backup_scheduler_row[0][0])

        cursor.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('heartbeat', ))
        last_heartbeat_row = cursor.fetchall()
        last_heartbeat = str(last_heartbeat_row[0][0])

        cursor.close()
        conn.close()

        print "ft_flow status is updated " + str(datetime.datetime.now().strftime(TIMESTAMP_FORMAT))
        return active_scheduler, active_backup_scheduler, last_heartbeat
    except (Exception, psycopg2.DatabaseError) as error:
    	print error
    return active_scheduler, active_backup_scheduler, last_heartbeat

def get_public_dns():
    """ Returns the public DNS name of the current node """
    out = subprocess.Popen(['curl', '-s', 'http://169.254.169.254/latest/meta-data/public-hostname'],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.STDOUT)
    stdout,stderr = out.communicate()
    return stdout


def run_split_command(command_split):
    """ Splits the command and returns a success/failure flag if the command has been run successfully """
    is_successful = True
    output = []
    try:
        process = subprocess.Popen(command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()

        if process.stderr is not None:
            stderr_output = process.stderr.readlines()
            if stderr_output and len(stderr_output) > 0:
                output += stderr_output
        if process.stdout is not None:
            output += process.stdout.readlines()
    except Exception, e:
        is_successful = False
        output = str(e)
        print 'Exception raised in run_split_command method with output: ' + str(output)

    return is_successful, output

def terminate_scheduler(node):
    """ Kills all active scheduler processes on a given node (that should be active scheduler) """
    current_node = get_public_dns()
    stop_command = "pkill -f 'airflow scheduler'"

    print "...Terminating scheduler on node '" + node + "'..."
    is_successful = True
    command_split = ["ssh", "-o ConnectTimeout=1", node, stop_command] # pkill -f 'airflow scheduler'
    is_successful, output = run_split_command(command_split=command_split)

    if is_successful:
        print "Active scheduler on " + str(node) + " has been terminated!"
    else:
        print "Termination for " + str(node) + " failed!"


def is_running(node_type, node):
    """ Checks if the given node is running active scheduler or active backup scheduler proces  """
    command = None

    if node_type == 'active_scheduler':
        command = 'airflow scheduler'

    if node_type == 'active_backup_scheduler':
        command = 'python ft_airflow.py'

    process_check_command = "ps -eaf"
    grep_command = "grep '" + str(command) + "'"
    grep_command_no_quotes = grep_command.replace("'", "")
    status_check_command = process_check_command + " | " + grep_command  # ps -eaf | grep 'airflow scheduler' or ps -eaf | grep 'python ft_airflow.py'
    is_running = False
    is_successful = True

    # SSH to the node to see if it is running 'airflow scheduler' process
    if status_check_command.startswith("sudo"):
        command_split = ["ssh", "-o ConnectTimeout=1", "-tt", node, status_check_command] # 1 sec timeout for ssh
    else:
        command_split = ["ssh", "-o ConnectTimeout=1", node, status_check_command] # ssh -o ConnectTimeout=1 <node_dns_name>

    is_successful, output = run_split_command(command_split=command_split)

    if is_successful:
        active_list = []
        for line in output:
            if line.strip() != "" and process_check_command not in line and grep_command not in line and grep_command_no_quotes not in line and status_check_command not in line and "timed out" not in line:
                active_list.append(line)

        active_list_length = len(filter(None, active_list))
        is_running = active_list_length > 0

    return is_running


# Get active and backup schedulers from DB, and also the last heartbeat.
active_scheduler,  active_backup_scheduler, last_heartbeat = get_ft_airflow_status()
scheduler_status = 'OFF'
if is_running('active_scheduler', active_scheduler):
    scheduler_status = 'ON'
backup_scheduler_status = 'OFF'
if is_running('active_backup_scheduler', active_backup_scheduler):
    backup_scheduler_status = 'ON'
########################

# Dash runs on Flask server
server = flask.Flask('app')
server.secret_key = os.environ.get('secret_key', 'secret')

# Get an initial dataframe provided by plotly. It is immideately overwritten by
# the stock prices fetched from the DB that stores the results of the datapipeline
df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/hello-world-stock.csv')
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash('app', server=server, external_stylesheets=external_stylesheets)
app.scripts.config.serve_locally = False
dcc._js_dist[0]['external_url'] = 'https://cdn.plot.ly/plotly-basic-latest.min.js'
app.layout = html.Div([
    html.H1('Fault-Tolerant Apache Airflow automating Daily Stock Prices'),

    html.Div(id='time_live', style=time_style),

    dcc.Textarea(
        id='live-update-text',
        placeholder='CurrentScheduler',
        value="Current Scheduler: " + str(active_scheduler) + ", Status: " + str(scheduler_status) +
        "\n\nCurrent Backup Scheduler: " + str(active_backup_scheduler) + ", Status: " + str(backup_scheduler_status) +
        "\n\nLast Heartbeat Time: " + str(last_heartbeat),
        style={'width': '100%', 'height': '100px', 'font-size': 'large'}
    ),


    html.Button("Terminate Active Scheduler", value='Terminate Active Scheduler', id='terminate_button', style=button_style),

    dcc.Dropdown(
        id='my-dropdown',
        options=[{'label': stock, 'value': stock} for stock in get_distinct_stocks()],
        value='SAP'
    ),

    dcc.Graph(id='my-graph'),

    dcc.Interval(
            id='interval-component',
            interval=1*page_refresh_period, # in milliseconds
            n_intervals=1
    ),

], className="container")


# Call backs for actions in the web page
@app.callback(Output('terminate_button', 'value'),
              [Input('terminate_button', 'n_clicks')])
def terminate_scheduler_callback(n_clicks):
    """ Kills the active scheduler process when 'Terminate Active Scheduler' is clicked """
    active_scheduler,  active_backup_scheduler, last_heartbeat = get_ft_airflow_status()
    terminate_scheduler(active_scheduler)


@app.callback(Output('time_live', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_time(n):
    return str(datetime.datetime.now().strftime(TIMESTAMP_FORMAT))

@app.callback(Output('live-update-text', 'value'),
              [Input('interval-component', 'n_intervals')])
def update_status(n):
    """ Updates the status of active and backup schedulers and the heartbeat """
    active_scheduler,  active_backup_scheduler, last_heartbeat = get_ft_airflow_status()
    scheduler_status = 'OFF'

    # Check active scheduler's status
    if is_running('active_scheduler', active_scheduler):
        scheduler_status = 'ON'

    backup_scheduler_status = 'OFF'

    # Check active backup scheduler's status
    if is_running('active_backup_scheduler', active_backup_scheduler):
        backup_scheduler_status = 'ON'

    return ("Current Scheduler: " + str(active_scheduler) + ", Status: " + str(scheduler_status) +
    "\n\nCurrent Backup Scheduler: " + str(active_backup_scheduler) + ", Status: " + str(backup_scheduler_status) +
    "\n\nLast Heartbeat Time: " + str(last_heartbeat))



@app.callback(Output('my-graph', 'figure'),
              [Input('my-dropdown', 'value')])
def update_graph(selected_dropdown_value):
    """ Updates the graph everytime a new stock is selected from the dropdown """
    conn = connect_db('pds_db')
    cursor = conn.cursor()
    cursor.execute("""SELECT "TradingDate", "AvgPrice" FROM avg_xetr WHERE "Ticker" = %s order by "TradingDate";""", (selected_dropdown_value, ))
    selected_ticker = cursor.fetchall()
    cursor.close()
    conn.close()
    trading_dates = [i[0] for i in selected_ticker]
    avg_prices = [i[1] for i in selected_ticker]

    dff = df[df['Stock'] == selected_dropdown_value]
    return {
        'data': [{
            'x': trading_dates,
            'y': avg_prices,
            'line': {
                'width': 3,
                'shape': 'spline'
            }
        }],
        'layout': {
            'margin': {
                'l': 30,
                'r': 20,
                'b': 30,
                't': 20
            }
        }
    }


if __name__ == '__main__':
    app.run_server(host='localhost', port=8000)
