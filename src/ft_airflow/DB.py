import os
import psycopg2
from datetime import datetime
import date_utils as du
from pytz import timezone


class DB:
    """ Database class that handles all metadata database related operations """

    # Datetime format to convert datetime to string
    DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


    def __init__(self):
        """ Creates a table called 'ft_airflow' to store the metadata of ft_airflow controller """
        try:
            self.connect_to_db()
            statement = """CREATE TABLE ft_airflow (key VARCHAR(255) PRIMARY KEY, value VARCHAR(255) NOT NULL);"""

            # create a new cursor
            cur = self.conn.cursor()
            cur.execute(statement)
            self.conn.commit()
            print "Table 'ft_airflow' has been created."

        except Exception, e:
            print "Table 'ft_airflow' could not be created. Because it has been already created!"

        cur.close()
        self.close_conn()

    def get_active_scheduler(self):
        """ Returns active scheduler node """
        try:
            self.connect_to_db()
            # create a new cursor
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('active_scheduler', ))
            row = cur.fetchall()

            # If the active scheduler is set, return it
            if len(row) is not 0:
                print str(row[0][0])
                return row[0][0]

            # If there is no active scheduler, return none
            else:
                return None

        except (Exception, psycopg2.DatabaseError) as error:
            print error

        cur.close()
        self.close_conn()

    def set_active_scheduler(self, scheduler_dns):
        """ Checks if the active scheduler is set. If not, it inserts a new one.
        If the active scheduler is set, it is updated"""

        try:
            self.connect_to_db()
            # create a new cursor
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('active_scheduler', ))
            row = cur.fetchall()

            # If the active scheduler is already set, update it
            if len(row) is not 0:
                update_statement = """UPDATE ft_airflow SET value = %s WHERE key = %s;"""
                cur = self.conn.cursor()
                cur.execute(update_statement, (scheduler_dns, "active_scheduler"))
                print "Active scheduler has been updated as " + str(scheduler_dns)

            # If the active scheduler is not set yet, insert it
            else:
                insert_statement = """INSERT INTO ft_airflow VALUES (%s, %s) RETURNING value;"""
                # create a new cursor
                cur = self.conn.cursor()

                # execute the INSERT statement
                cur.execute(insert_statement, ("active_scheduler", scheduler_dns))

                # get the generated value back
                return_value = cur.fetchone()[0]
                print "Active scheduler has been set as " + str(return_value)

            # commit the changes to the database
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print error

        cur.close()
        self.close_conn()

    def get_active_backup_scheduler(self):
        """Returns active backup scheduler if there is any"""
        try:
            self.connect_to_db()
            # create a new cursor
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('active_backup_scheduler',))
            row = cur.fetchall()

            # If the active backup scheduler is already set, return it
            if len(row) is not 0:
                return row[0][0]

            # If there is no active backup scheduler, return none
            else:
                return None

        except (Exception, psycopg2.DatabaseError) as error:
            print error

        cur.close()
        self.close_conn()


    def set_active_backup_scheduler(self, backup_scheduler_dns):
        """ Checks if the active backup scheduler is set. If not, it inserts a new one.
        If the active backup scheduler is set, it is updated"""

        try:
            self.connect_to_db()
            # create a new cursor
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('active_backup_scheduler', ))
            row = cur.fetchall()

            # If the active scheduler is already set, update it
            if len(row) is not 0:
                update_statement = """UPDATE ft_airflow SET value = %s WHERE key = %s;"""
                cur = self.conn.cursor()
                cur.execute(update_statement, (backup_scheduler_dns, "active_backup_scheduler"))
                print "Active backup scheduler has been updated as " + str(backup_scheduler_dns)

            # If the active scheduler is not set yet, insert it
            else:
                insert_statement = """INSERT INTO ft_airflow VALUES (%s, %s) RETURNING value;"""
                # create a new cursor
                cur = self.conn.cursor()

                # execute the INSERT statement
                cur.execute(insert_statement, ("active_backup_scheduler", backup_scheduler_dns))

                # get the generated value back
                return_value = cur.fetchone()[0]
                print "Active backup scheduler has been set as " + str(return_value)

            # commit the changes to the database
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print error

        cur.close()
        self.close_conn()

    def get_heartbeat(self):
        """ Returns the heartbeat time """
        try:
            self.connect_to_db()
            # create a new cursor
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('heartbeat', ))
            row = cur.fetchall()

            # If there is a time for the last heartbeat, return it
            if len(row) is not 0:
                eastern = timezone('US/Eastern')
                return eastern.localize(du.get_string_as_datetime(row[0][0]))
                #return du.get_string_as_datetime(row[0][0])

            # If there is no time for the heartbeat, return none
            else:
                return None

        except (Exception, psycopg2.DatabaseError) as error:
            print error

        cur.close()
        self.close_conn()

    def set_heartbeat(self):
        """ Checks if the last heartbeat time is set. If not, it inserts a new one.
        If the last heartbeat time is already set, it is updated"""

        try:
            self.connect_to_db()
            # create a new cursor
            cur = self.conn.cursor()
            cur.execute("""SELECT value FROM ft_airflow WHERE key = %s;""", ('heartbeat', ))
            row = cur.fetchall()

            eastern = timezone('US/Eastern')
            heartbeat_datetime = du.get_datetime_as_str(datetime.now(eastern))

            # If the heartbeat is already set, update it
            if len(row) is not 0:
                update_statement = """UPDATE ft_airflow SET value = %s WHERE key = %s;"""
                cur = self.conn.cursor()
                cur.execute(update_statement, (heartbeat_datetime, "heartbeat"))
                print "The last heartbeat is set as " + str(heartbeat_datetime)

            # If the heartbeat is not set yet, insert it
            else:
                insert_statement = """INSERT INTO ft_airflow VALUES (%s, %s) RETURNING value;"""
                # create a new cursor
                cur = self.conn.cursor()

                # execute the INSERT statement
                cur.execute(insert_statement, ("heartbeat", heartbeat_datetime))

                # get the generated value back
                return_value = cur.fetchone()[0]
                print "The last heartbeat has been set as " + str(return_value)

            # commit the changes to the database
            self.conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print error
        cur.close()
        self.close_conn()


    def connect_to_db(self):
        try:
            # connect to the PostgreSQL database
            self.conn = psycopg2.connect(
                host=os.environ.get('PG_HOST'),
                database='airflow',
                user=os.environ.get('PG_USER'),
                password=os.environ.get('PG_PSWD')
                )
        except (Exception, psycopg2.DatabaseError) as error:
            print error


    def close_conn(self):
        """Closes the connection to DB"""
        try:
            self.conn.close()
            # print "Database connection is closed."
        except (Exception, psycopg2.DatabaseError) as error:
            print error
