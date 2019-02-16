import os
import subprocess
import ConfigParser
import socket
import sys

def get_airflow_home_dir():
    """Function that returns the home directory of airflow."""

    if "AIRFLOW_HOME" in os.environ:
        return os.environ['AIRFLOW_HOME']
    else:
        return os.path.expanduser("~/airflow")

# Command used to start airflow scheduler.
# nohup guarantees that airflow scheduler process does not get killed if
# logout from session. Output will be appended to scheduler.logs
START_COMMAND = "nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &"

# Command used to stop airflow scheduler.
# The output of ps -ef | grep 'airflow scheduler' command is piped to awk to get the process ID and kill it.
STOP_COMMAND = "for pid in `ps -ef | grep 'airflow scheduler' | awk '{print $2}'` \; do kill -9 $pid \; done"


# Variable that stores airflow home directory
AIRFLOW_HOME = get_airflow_home_dir()

# This is the variable how frequent the active scheduler polls the scheduler in standby mode in secs
HEARTBEAT_FREQUENCY = 5

# A list storing the possible schedulers. In the case of one active one stand by mode scheduler,
# the list has two elements in it
SCHEDULER_NODES = list()

class Config:
    """Configuration class that parses and stores the configuration of airflow defined in airflow.cfg file"""

    def __init__(self, airflow_home = None, airflow_config_path = None):
        """
        Initialize newly created Config instance by storing airflow_home
        and airflow_config_path to be used later
        """
        print "Configuring fault tolerant airflow scheduler..."

        if airflow_home is None:
            airflow_home = AIRFLOW_HOME

        if airflow_config_path is None:
            airflow_config_path = airflow_home + "/airflow.cfg"

        self.airflow_home = airflow_home
        self.airflow_config_path = airflow_config_path

        if not os.path.isfile(airflow_config_path):
            print "No airflow configuration file at <" + str(airflow_config_path) + ">. Exiting..."
            sys.exit(1)

        # If airflow config file (i.e., airflow.cfg) is found, read it
        self.conf = ConfigParser.RawConfigParser()
        self.conf.read(airflow_config_path)

        self.read_schedulers()


    def read_schedulers(self):
        """
        Reads candidate scheduler nodes from schedulers.txt file
        and appends them to SCHEDULER_NODES list
        """
        f = open("schedulers.txt", "r")
        for line in f:
            SCHEDULER_NODES.append(line.rstrip('\n'))


    def get_scheduler_nodes(self):
        """ Returns scheduler nodes, one active, one in standby mode """
        return SCHEDULER_NODES

    def get_heartbeat_frequency(self):
        """ Returns the heartbeat frequency in secs"""
        return HEARTBEAT_FREQUENCY

    def get_config(self, section, field):
        """
        Returns a config value of a specific section and field of the airflow configuration file

        Input: Config section and the key of the field
        (Example: Use get_config("core", "sql_alchemy_conn") to get database connector)

        Output: Corresponding config value for section and field
        """
        config_value = self.conf.get(section, field)
        return config_value


    def get_sql_alchemy_conn(self):
        """ Returns the connection string from airflow configuration file """
        return self.get_config("core", "sql_alchemy_conn")


    def get_airflow_home(self):
        """ Returns airflow main directory """
        return AIRFLOW_HOME

    def get_hostname(self):
        """ Returns the local host name of the current node """
        return socket.gethostname()


    def get_public_dns(self):
        """ Returns the public DNS name of the current node """
        out = subprocess.Popen(['curl', '-s', 'http://169.254.169.254/latest/meta-data/public-hostname'],
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)

        stdout,stderr = out.communicate()
        return stdout


    def get_start_command(self):
        """ Returns the start command of airflow scheduler """
        return START_COMMAND

    def get_stop_command(self):
        """ Returns the stop command of airflow scheduler """
        return STOP_COMMAND.replace("\\;", ";")
