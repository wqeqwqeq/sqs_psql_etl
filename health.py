from sqlalchemy import create_engine


def health_check():
    db_name = "postgres"
    db_user = "postgres"
    db_pass = "postgres"
    db_host = "localhost"
    db_port = "5432"

    # Connecto to the database
    db_string = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
        db_user, db_pass, db_host, db_port, db_name
    )
    conn = create_engine(db_string)
    result = conn.execute("select count(*) from user_logins")
    print("Number of rows in user_logins now is", result.fetchall()[0][0])


if __name__ == "__main__":
    health_check()
