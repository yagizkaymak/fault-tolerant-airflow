"""
This is the main class of the fault tolerant airflow application.
Each node, one active, one in stand by mode, has to run this application one at a time.
Before running the application, please make sure that 'schedulers.txt'
file has the public DNS names, each as a seperate line, of the candidate schedulers.

This application is developed as an Insight data science project.

Author: Yagiz Kaymak
Date: February, 2019
"""

import os
import subprocess
import datetime
import time
from config import Config # Importing script that stores configuration variables
from DB import DB # Importing sript that handles database operations

config = Config()
db = DB()


def main():
    #sql_alchemy_conn = config.get_sql_alchemy_conn()
    #print sql_alchemy_conn

    ft = ft_airflow()

    while 1:
        ft.poll()
        print "------------ Finished Polling. Sleeping for " + str(config.get_heartbeat_frequency()) + " seconds ------------"
        time.sleep(config.get_heartbeat_frequency())


    #ft.start_scheduler('ec2-18-235-191-19.compute-1.amazonaws.com')
    #ft.terminate_scheduler('ec2-18-235-191-19.compute-1.amazonaws.com')
    #ft.is_running('ec2-18-235-191-19.compute-1.amazonaws.com')
    # ft.is_running('ec2-34-239-217-105.compute-1.amazonaws.com')


class ft_airflow:

    backup_active_flag = False  # Flag that shows if the node is active as scheduler

    def __init__(self):
        self.current_node = config.get_public_dns() # Get the public DNS name of current node
        self.scheduler_nodes = config.get_scheduler_nodes()
        self.start_command = config.get_start_command()
        self.stop_command = config.get_stop_command()
        self.heartbeat_frequency = config.get_heartbeat_frequency()
        print "-------------- Fault-tolerant airflow scheduler is running on " + self.current_node + " --------------"


    def poll(self):
        active_scheduler = db.get_active_scheduler()
        backup_scheduler = db.get_active_backup_scheduler()
        last_heartbeat = db.get_heartbeat()
        current_time = datetime.datetime.now()

        print "Active scheduler: " + str(active_scheduler) + ", Backup scheduler: " + str(backup_scheduler) + ", Last heartbeat: " + str(last_heartbeat)


        # If this backup node is not active, then execute this statement
        if not self.backup_active_flag:
            print "This backup node is on STANDBY mode"

            # If a backup scheduler is already set in DB
            if backup_scheduler is not None:
                print "There is already an active backup scheduler: " + str(backup_scheduler)

                # If the active backup scheduler is found as this node, activate it as a backup controller
                if backup_scheduler == self.current_node:
                    print "Backup node is set as this node. This node should be active as backup controller!"
                    self.set_backup_scheduler_active()

                # If the active backup scheduler in DB is not this node and there is no heartbeat in DB
                # set this node as active backup scheduler
                elif last_heartbeat is None:
                    print "Last heartbeat is None"
                    self.set_backup_scheduler_active()
                    backup_scheduler = db.get_active_backup_scheduler()
                # Check if the heartbeat differance is more than the double of the polling frequency
                # If so, this means the active backup scheduler has not refreshed the heartbeat more than the double of polling frequency
                # Therefore, this node takes over and announces itself as backup scheduler.
                else:
                    failover_heartbeat_diff = (current_time - last_heartbeat).seconds
                    print "Backup controller heartbeat difference: " + str(failover_heartbeat_diff) + " seconds"
                    max_age = self.heartbeat_frequency * 2
                    print "Heartbeat Max Age: " + str(max_age) + " seconds"

                    # Set the
                    if failover_heartbeat_diff > max_age:
                        print "Heartbeat " + db.convert_datetime_to_string(last_heartbeat) + " for active backup controller " + str(backup_scheduler) + " is older then max age of " + str(max_age) + " seconds"
                        self.set_backup_scheduler_active()
                        backup_scheduler = db.get_active_backup_scheduler()
            # If backup scheduler in DB is None
            else:
                print "Backup scheduler is None"
                self.set_backup_scheduler_active()
                backup_scheduler = db.get_active_backup_scheduler()

        # If this backup controller is active
        if self.backup_active_flag:

            # If the backup scheduler in DB is different than this node
            # and this node is active as a backup scheduler, make it inactive
            if backup_scheduler != config.get_public_dns():
                print "This backup scheduler can not be active! DB indicates that the active backup scheduler is " + str(backup_scheduler) + ". Public DNS of this node: " + config.get_public_dns()
                self.set_backup_scheduler_inactive()

            # If the backup scheduler in DB matches with this node's DNS name, start polling
            else:
                print "Setting heartbeat"

                # Set the heartbeat time in DB
                db.set_heartbeat()

                # If the active scheduler is not set yet, search for the active one
                # and store it in DB
                if active_scheduler is None:
                    print "Active scheduler is None"
                    active_scheduler = self.find_active_scheduler()
                    db.set_active_scheduler(active_scheduler)

                # If there is an active scheduler in DB
                else:
                    # If this active scheduler is not listed in the candidate scheduler list
                    # search for a new active scheduler
                    if active_scheduler not in self.scheduler_nodes:
                        print "Active scheduler is not in the scheduler_nodes list. Finding a new active scheduler."
                        active_scheduler = self.find_active_scheduler()
                        db.set_active_scheduler(active_scheduler)

                # If the active scheduler is not running, run it
                if not self.is_running(active_scheduler):
                    print "Scheduler is not running on " + str(active_scheduler)
                    self.start_scheduler(active_scheduler)
                    print "Sleep for 3 secs until scheduler starts"
                    time.sleep(3)

                    # If active scheduler is still not running after the attemp to run it
                    if not self.is_running(active_scheduler):
                        print "Failed to restart scheduler on " + str(active_scheduler)
                        print "Starting to search for a new active scheduler"
                        is_successful = False

                        # Find possible standby nodes that are different than this node and try to run the first one.
                        for standby_node in self.get_standby_nodes(active_scheduler):
                            print "Trying to start scheduler on STANDBY node " + str(standby_node)
                            self.start_scheduler(standby_node)
                            if self.is_running(standby_node):
                                is_successful = True
                                active_scheduler = standby_node
                                db.set_active_scheduler(active_scheduler)
                                print "New active scheduler is set to " + str(active_scheduler)
                                break
                    else:
                        print "Confirmed the scheduler is now running"

                # If an active scheduler is running
                else:
                    print "Checking if scheduler instances are running on STANDBY nodes..."

                    for standby_node in self.get_standby_nodes(active_scheduler):
                        if self.is_running(standby_node):
                            print "There is a scheduler running on STANDBY node " + standby_node + ". Shutting down that scheduler."
                            self.terminate_scheduler(standby_node)
                    print "Finished checking if scheduler instances are running on STANDBY nodes"

        else:
            print "This backup controller is on STANDBY mode"

    def start_airflow(self, command):
        print "'" + command + "' is running..."
        output = os.popen(config.get_airflow_home()).read()
        if output:
            output = output.split("\n")


    def is_running(self, node):
        process_check_command = "ps -eaf"
        grep_command = "grep 'airflow scheduler'"
        grep_command_no_quotes = grep_command.replace("'", "")
        status_check_command = process_check_command + " | " + grep_command  # ps -eaf | grep 'airflow scheduler'
        is_running = False
        is_successful = True

        # Local command run
        if node == self.current_node:
            output = os.popen(status_check_command).read()
            if output:
                output = output.split("\n")
                is_successful = True
            print "Finished Checking if scheduler on LOCAL host " + str(node) + " is running. is_running: " + str(is_running)
        # Remote command run
        else:
            if status_check_command.startswith("sudo"):
                command_split = ["ssh", "-o ConnectTimeout=1", "-tt", node, status_check_command]
            else:
                command_split = ["ssh", "-o ConnectTimeout=1", node, status_check_command]

            is_successful, output = self.run_split_command(command_split=command_split)

            print "Finished Checking if scheduler on REMOTE host " + str(node) + " is running. is_running: " + str(is_running)


        if is_successful:
            active_list = []
            for line in output:
                if line.strip() != "" and process_check_command not in line and grep_command not in line and grep_command_no_quotes not in line and status_check_command not in line and "timed out" not in line:
                    active_list.append(line)

            active_list_length = len(filter(None, active_list))

            is_running = active_list_length > 0
        else:
            print "is_running check failed"

        print "Finished Checking if scheduler on host " + str(node) + " is running. is_running: " + str(is_running)

        return is_running


    def heartbeat(self):
        print "Polling has started on " + self.current_node


    def find_active_scheduler(self):
        active_scheduler = None
        print "...Finding active scheduler..."
        active_schedulers = []

        for scheduler in self.scheduler_nodes:
            if self.is_running(scheduler):
                active_schedulers.append(scheduler)

        print "Nodes with a scheduler currently running on it: " + str(active_schedulers)

        # If there are more than 1 active schedulers running terminate except the first one
        if len(active_schedulers) > 1:
            print "Multiple nodes have a scheduler running on it. Terminating all schedulers except the first one!"

            for index, node in enumerate(active_schedulers):
                if index != 0:
                    self.terminate_scheduler(node)
                else:
                    active_scheduler = node
        # If the number of active schedulers is 1 set 'active_scheduler' variable to return
        elif len(active_schedulers) == 1:
            print "One active scheduler is found as " + str(active_schedulers[0])
            active_scheduler = active_schedulers[0]
        else:
            print "There is no active scheduler running. Use the first scheduler running in the schedulers list."
            for scheduler in self.scheduler_nodes:

                # Local command run
                if scheduler == self.current_node:
                    return scheduler

                # Remote command run
                else:
                    command = "echo 'Connection Succeeded'"
                    print "REMOTE find_active_scheduler COMMAND"
                    if command.startswith("sudo"):
                        command_split = ["ssh", "-o ConnectTimeout=1", "-tt", scheduler, command]
                    else:
                        command_split = ["ssh", "-o ConnectTimeout=1", scheduler, command]

                    is_successful, output = self.run_split_command(command_split=command_split)

                    if "timed out" not in output:
                        return scheduler


        print "Finished searching for active scheduler. Active scheduler is : " + str(active_scheduler)
        return active_scheduler

    def terminate_scheduler(self, node):
        print "...Terminating scheduler on node '" + node + "'..."
        is_successful = True

        # Local command run
        if node == self.current_node:
            output = os.popen(self.stop_command).read()
            if output:
                output = output.split("\n")
                is_successful = True
        # Remote command run
        else:
            if self.stop_command.startswith("sudo"):
                command_split = ["ssh", "-o ConnectTimeout=1", "-tt", node, self.stop_command]
            else:
                command_split = ["ssh", "-o ConnectTimeout=1", node, self.stop_command]

            is_successful, output = self.run_split_command(command_split=command_split)

            if "timed out" not in output:
                if is_successful:
                    print "Active scheduler on " + str(node) + " has been terminated!"
                else:
                    print "Termination for " + str(node) + " failed!"
            else:
                print "Termination for " + str(node) + " failed because of a connection time out!"


    def start_scheduler(self, node):
        print "..... Starting scheduler on node " + str(node) + " ....."
        is_successful = True

        # Local command run
        if node == self.current_node:
            output = os.popen(self.start_command).read()
            if output:
                output = output.split("\n")
                is_successful = True
        # Remote command run
        else:
            if self.start_command.startswith("sudo"):
                command_split = ["ssh", "-o ConnectTimeout=1", "-tt", node, self.start_command]
            else:
                command_split = ["ssh", "-o ConnectTimeout=1", node, self.start_command]

            is_successful, output = self.run_split_command(command_split=command_split)


            if "timed out" not in output:
                if is_successful:
                    print "Scheduler has been started  on " + str(node)
                else:
                    "Scheduler start for " + str(node) + " failed!"
            else:
                print "Scheduler start for " + str(node) + " failed because of a connection time out!"


    def run_split_command(self, command_split):
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


    def get_standby_nodes(self, active_scheduler):
        """ Returns all nodes except this one as standby nodes """
        standby_nodes = []

        for node in self.scheduler_nodes:
            if node != active_scheduler:
                standby_nodes.append(node)
        return standby_nodes


    def set_backup_scheduler_active(self):
        """ Sets the backup controller active """

        print "Setting this node as ACTIVE"
        try:
            db.set_active_backup_scheduler(self.current_node)
            db.set_heartbeat()
            self.backup_active_flag = True
            print "This backup controller is now ACTIVE."
        except Exception, e:
            self.backup_active_flag = False
            print "Failed to set this backup controller as ACTIVE. Trying again next heart beat."

    def set_backup_scheduler_inactive(self):
        print "Setting this backup controller to INACTIVE"
        self.backup_active_flag = False

if __name__=="__main__":
    main()
