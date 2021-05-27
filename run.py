import CCM  # The moment CCM is imported its __init__ is called; hence the configuration settings are completed and we have app.config values
import cx_Oracle
from CCM.app_scope_methods import controls_per_engine
import logging, time, threading
from CCM.app_scope_methods import job_handler

logger = logging.getLogger(__name__)
print(logger.parent, 'parent of run logger')

# cx_Oracle.init_oracle_client(lib_dir=r"C:\ORACLEINSTANTCLIENT\instantclient_19_10")
cx_Oracle.init_oracle_client(lib_dir=CCM.app.config["ORACLE_CLIENT_PATH"])


def thread_function(name):
    while True:
        logger.info("Thread %s: starting", name)
        job_handler.list_of_jobs_to_be_handled()
        print("Thread %s: starting", name)
        time.sleep(20)
        print("Thread %s: finishing", name)


def create_ccm_app():
    # CCM.app.debug = True # This is loaded from the config as DEBUG = True
    CCM.db.create_all(app=CCM.app)   # This creates the DB

    # After the app been configured calling the checks for the Controls and Engine_ID association
    # maintains what all controls are to be handled by the particular application

    controls_per_engine.list_of_controls_per_engine()

    # start a job handler thread
    x = threading.Thread(target=thread_function, args=(1,), daemon=True)
    logger.info("Main : before running thread")
    x.start()

    CCM.app.run()


if __name__ == "__main__":
    print(__name__)
    # app.debug = True
    # db.create_all(app=app)
    # app.run()
    create_ccm_app()