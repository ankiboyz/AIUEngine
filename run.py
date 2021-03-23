from CCM import db, app


if __name__ == "__main__" :
    print(__name__)
    app.debug = True
    db.create_all(app=app)
    app.run()