import CCM


def create_ccm_app():
    # CCM.app.debug = True # This is loaded from the config as DEBUG = True
    CCM.db.create_all(app=CCM.app)
    CCM.app.run()


if __name__ == "__main__" :
    print(__name__)
    # app.debug = True
    # db.create_all(app=app)
    # app.run()
    create_ccm_app()