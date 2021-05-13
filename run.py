import CCM  # The moment CCM is imported its __init__ is called; hence the configuration settings are completed and we have app.config values
import cx_Oracle
from CCM.app_scope_methods import controls_per_engine


# cx_Oracle.init_oracle_client(lib_dir=r"C:\ORACLEINSTANTCLIENT\instantclient_19_10")
cx_Oracle.init_oracle_client(lib_dir=CCM.app.config["ORACLE_CLIENT_PATH"])


def create_ccm_app():
    # CCM.app.debug = True # This is loaded from the config as DEBUG = True
    CCM.db.create_all(app=CCM.app)   # This creates the DB

    # After the app been configured calling the checks for the Controls and Engine_ID association
    controls_per_engine.list_of_controls_per_engine()

    CCM.app.run()




if __name__ == "__main__":
    print(__name__)
    # app.debug = True
    # db.create_all(app=app)
    # app.run()
    create_ccm_app()