import BCM  # The moment BCM is imported its __init__ is called; hence the configuration settings are completed and we have app.config values
import cx_Oracle
from BCM.app_scope_methods import controls_per_engine
import logging, time, threading
from BCM.app_scope_methods import job_handler

logger = logging.getLogger(__name__)
print(logger.parent, 'parent of run logger')

# cx_Oracle.init_oracle_client(lib_dir=r"C:\ORACLEINSTANTCLIENT\instantclient_19_10")
# Commenting from here as it was giving the library already initialized error as shown below and moving it in
# the if __main__ block; so that import of this run wont be called again.
# Traceback (most recent call last):
#   File "C:/Users/ASaxena/PycharmProjects/AIUEngine/run.py", line 50, in <module>
#     cx_Oracle.init_oracle_client(lib_dir=BCM.app.config["ORACLE_CLIENT_PATH"])
# cx_Oracle.ProgrammingError: Oracle Client library has already been initialized

# cx_Oracle.init_oracle_client(lib_dir=BCM.app.config["ORACLE_CLIENT_PATH"])


def thread_function(name):
    while True:
        logger.info("JOB_HANDLER_THREAD Thread %s: starting", name)
        job_handler.list_of_jobs_to_be_handled()
        logger.debug(f' The JSM JOB_HANDLER_THREAD Main Daemon Thread loop starting')
        # print("Thread %s: starting", name)
        time.sleep(BCM.app.config["JOB_HANDLER_THREAD_POLL_FREQUENCY"])      # make it configurable the sleep time of main daemon
        logger.debug(f' The JSM JOB_HANDLER_THREAD Main Daemon Thread loop finishing')
        logger.info("JOB_HANDLER_THREAD Thread %s: finishing", name)


def create_ccm_app():
    # BCM.app.debug = True # This is loaded from the config as DEBUG = True
    BCM.db.create_all(app=BCM.app)   # This creates the DB

    # After the app been configured calling the checks for the Controls and Engine_ID association
    # maintains what all controls are to be handled by the particular application

    controls_per_engine.list_of_controls_per_engine()

    # start a job handler thread
    x = threading.Thread(target=thread_function, args=("JOB_HANDLER_THREAD",), daemon=True)
    logger.info("Main : JOB_HANDLER_THREAD before running thread")
    x.start()

    return BCM.app, BCM.db  # here it returns the initialized db as well.


if __name__ == "__main__":
    print(__name__)
    # app.debug = True
    # db.create_all(app=app)
    # app.run()

    # initializing the cx_Oracle client to connect to the Oracle database.
    cx_Oracle.init_oracle_client(lib_dir=BCM.app.config["ORACLE_CLIENT_PATH"])

    appln, db = create_ccm_app()    # as it returns the tuple of application and db
    try:
        appln.run()
    finally:
        logger.info("Finally block in the application stop ")
        print("Finally block in the application stop ")  # in case logger is not initialized when the application stops