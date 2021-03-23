import CCM

def create_CCM_app():
    CCM.app.debug = True
    CCM.db.create_all(app=CCM.app)
    CCM.app.run()


if __name__ == "__main__" :
    print(__name__)
    # app.debug = True
    # db.create_all(app=app)
    # app.run()
    create_CCM_app()