from typing import List, Tuple
from psycopg2 import sql

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Movie import Movie
from Business.Studio import Studio
from Business.Critic import Critic
from Business.Actor import Actor


# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("BEGIN;"

                              "CREATE TABLE Critics("
                              "criticID INTEGER PRIMARY KEY, "
                              "critic_name TEXT NOT NULL, "
                              "CHECK (criticID > 0); "
                              
                              #TODO: change this table
                              "CREATE TABLE Movies("
                              "movieName TEXT NOT NULL, "
                              "movie_year INTEGER NOT NULL,"
                              "movie_genre TEXT NOT NULL, "
                              "CHECK (movie_year >= 1895), CHECK (movie_genre IN ['Drama', 'Action', 'Comedy', 'Horror'])"
                              "CONSTRAINT Movies_key PRIMARY KEY (movieName, movie_year)); "

                              "CREATE TABLE Actors("
                              "actorID INTEGER PRIMARY KEY,"
                              "actor_name TEXT NOT NULL, "
                              "actor_age INTEGER NOT NULL,"
                              "age_height INTEGER NOT NULL,"
                              "CHECK (actorID > 0), CHECK (actor_age > 0), CHECK (actor_height > 0));"

                              "CREATE TABLE Studios("
                              "studioID INTEGER PRIMARY KEY, "
                              "studio_name TEXT NOT NULL, "
                              "CHECK (studioID > 0); "

                              "CREATE TABLE Productions("
                              "production_budget INTEGER NOT NULL CHECK (production_budget >= 0), "
                              "production_revenue INTEGER NOT NULL CHECK (production_revenue >=0), "
                              "FOREIGN KEY (studioID) REFERENCES Studios(studioID) ON DELETE CASCADE, "
                              "FOREIGN KEY (movieName) REFERENCES Movies(movieName) ON DELETE CASCADE, "
                              "FOREIGN KEY (movie_year) REFERENCES Movies(movie_year) ON DELETE CASCADE, "
                              "CONSTRAINT Productions_key PRIMARY KEY (movie_name, movie_year)); "

                              "CREATE TABLE Reviews("
                              "review_rating INTEGER NOT NULL CHECK (review_rating > 0), CHECK (review_rating < 6), "
                              "FOREIGN KEY (criticID) REFERENCES Critics(criticID) ON DELETE CASCADE, "
                              "FOREIGN KEY (movieName) REFERENCES Movies(movieName) ON DELETE CASCADE, "
                              "FOREIGN KEY (movie_year) REFERENCES Movies(movie_year) ON DELETE CASCADE, "
                              "CONSTRAINT Reviews_key PRIMARY KEY (movie_name, movie_year, criticID)); "
                              
                              #TODO: change this table
                              "CREATE TABLE ActingJobs("
                              "job_salary INTEGER NOT NULL CHECK (job_salary > 0), "
                              "job_roles TEXT NOT NULL, "
                              "FOREIGN KEY (actorID) REFERENCES Actors(actorID) ON DELETE CASCADE, "
                              "FOREIGN KEY (movieName) REFERENCES Movies(movieName) ON DELETE CASCADE, "
                              "FOREIGN KEY (movie_year) REFERENCES Movies(movie_year) ON DELETE CASCADE, "
                              "CONSTRAINT Jobs_key PRIMARY KEY (movie_name, movie_year, actorID));"

                              # TODO: figure out correct group by
                              "CREATE VIEW CriticToStudio AS "
                              "SELECT V1.criticID, V1.studioID, COUNT(?)"
                              "FROM (Production INNER JOIN Reviews"
                              "ON (Production.movie_name = Reviews.movie_name AND Production.movie_year = Reviews.movie_year)) AS V1 "
                              "GROUP BY V1.criticID, V1.studioID;"

                              "COMMIT;")

        conn.execute(transaction)
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("BEGIN;"

                              "DELETE FROM Critics; "

                              "DELETE FROM Movies; "

                              "DELETE FROM Actors;"

                              "DELETE FROM Studios; "

                              "COMMIT;")

        conn.execute(transaction)
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
    except Exception as e:
        conn.rollback()
        print(e)
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("BEGIN;"

                              # "DROP VIEW QueriesRunOnMultipleDisks; "

                              "DROP VIEW CriticsToStudio; "

                              "DROP TABLE ActingJobs; "
                              
                              "DROP TABLE Productions; "

                              "DROP TABLE Reviews; "

                              "DROP TABLE Critics; "

                              "DROP TABLE Movies;"

                              "DROP TABLE Actors; "

                              "DROP TABLE Studios; "

                              "COMMIT;")

        conn.execute(transaction)
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
    except Exception as e:
        conn.rollback()
        print(e)
    finally:
        conn.close()


def addCritic(critic: Critic) -> ReturnValue:
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Critics "
                        "VALUES({critic_id}, {critic_name});").format(
            critic_id=sql.Literal(critic.getCriticID()),
            critic_name=sql.Literal(critic.getName()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.ALREADY_EXISTS
    except Exception:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
    return return_value


def deleteCritic(critic_id: int) -> ReturnValue:
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Critics "
                        "WHERE criticID = {id};").format(
            id=sql.Literal(
                critic_id))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return_value = ReturnValue.NOT_EXISTS
    except Exception as e:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
    return return_value


def getCriticProfile(critic_id: int) -> Critic:
    conn = None
    critic = Critic()
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT * "
                            "FROM Critics "
                            "WHERE criticID = {id};").format(
            id=sql.Literal(critic_id))
        _, result = conn.execute(sql_query)
        conn.commit()
        if result.isEmpty():
            critic = critic.badCritic()
        else:
            critic.setCriticID(int(result[0]['criticID']))
            critic.setName(str(result[0]['critic_name']))
    except Exception:
        critic = critic.badCritic()
    finally:
        conn.close()
    return critic


def addActor(actor: Actor) -> ReturnValue:
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Actors "
            "VALUES({actor_id}, {actor_name}, {actor_age}, {actor_height});").format(
            actor_id=sql.Literal(actor.getActorID()),
            actor_name=sql.Literal(actor.getActorName()),
            actor_age=sql.Literal(actor.getAge()),
            actor_height=sql.Literal(actor.getHeight()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.ALREADY_EXISTS
    except Exception:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
    return return_value


def deleteActor(actor_id: int) -> ReturnValue:
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Actors "
                        "WHERE actorID = {id};").format(
            id=sql.Literal(
                actor_id))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return_value = ReturnValue.NOT_EXISTS
    except Exception as e:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
    return return_value


def getActorProfile(actor_id: int) -> Actor:
    conn = None
    actor = Actor()
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT * "
                            "FROM Actors "
                            "WHERE actorID = {id};").format(
            id=sql.Literal(actor_id))
        _, result = conn.execute(sql_query)
        conn.commit()
        if result.isEmpty():
            actor = actor.badActor()
        else:
            actor.setActorID(int(result[0]['actorID']))
            actor.setActorName(str(result[0]['actor_name']))
            actor.setAge(int(result[0]['actor_age']))
            actor.setHeight(int(result[0]['actor_height']))
    except Exception:
        actor = actor.badActor()
    finally:
        conn.close()
    return actor


def addMovie(movie: Movie) -> ReturnValue:
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Movies "
            "VALUES({movieName}, {movie_year}, {movie_genre});").format(
            movieName=sql.Literal(movie.getMovieName()),
            movie_year=sql.Literal(movie.getYear()),
            movie_genre=sql.Literal(movie.getGenre()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.ALREADY_EXISTS
    except Exception:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
    return return_value


def deleteMovie(movie_name: str, year: int) -> ReturnValue:
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Movies "
                        "WHERE (movieName = {id1} AND movie_year == {id2});").format(
            id1=sql.Literal(movie_name),
            id2=sql.Literal(year))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return_value = ReturnValue.NOT_EXISTS
    except Exception as e:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
    return return_value


def getMovieProfile(movie_name: str, year: int) -> Movie:
    conn = None
    movie = Movie()
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT * "
                            "FROM Movie "
                            "WHERE (movieName = {id1} AND movie_year = {id2});").format(
            id1=sql.Literal(movie_name),
            id2=sql.Literal(year))
        _, result = conn.execute(sql_query)
        conn.commit()
        if result.isEmpty():
            movie = movie.badMovie()
        else:
            movie.setMovieName(str(result[0]['movieName']))
            movie.setYear(int(result[0]['movie_year']))
            movie.setGenre(int(result[0]['movie_genre']))
    except Exception:
        movie = movie.badMovie()
    finally:
        conn.close()
    return movie


def addStudio(studio: Studio) -> ReturnValue:
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Studios "
            "VALUES({studio_id}, {studio_name});").format(
            studio_id=sql.Literal(studio.getStudioID()),
            studio_name=sql.Literal(studio.getStudioName()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return_value = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return_value = ReturnValue.ALREADY_EXISTS
    except Exception:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
    return return_value


def deleteStudio(studio_id: int) -> ReturnValue:
    conn = None
    return_value = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Studios "
                        "WHERE studioID = {id};").format(
            id=sql.Literal(
                studio_id))
        rows_effected, _ = conn.execute(query)
        conn.commit()
        if rows_effected == 0:
            return_value = ReturnValue.NOT_EXISTS
    except Exception as e:
        return_value = ReturnValue.ERROR
    finally:
        conn.close()
    return return_value


def getStudioProfile(studio_id: int) -> Studio:
    conn = None
    studio = Studio()
    try:
        conn = Connector.DBConnector()
        sql_query = sql.SQL("SELECT * "
                            "FROM Studios "
                            "WHERE studioID = {id};").format(
            id=sql.Literal(studio_id))
        _, result = conn.execute(sql_query)
        conn.commit()
        if result.isEmpty():
            studio = studio.badStudio()
        else:
            studio.setStudioID(int(result[0]['studioID']))
            studio.setStudioName(str(result[0]['studio_name']))
    except Exception:
        studio = studio.badStudio()
    finally:
        conn.close()
    return studio


def criticRatedMovie(movieName: str, movieYear: int, criticID: int, rating: int) -> ReturnValue:
    # TODO: implement
    pass


def criticDidntRateMovie(movieName: str, movieYear: int, criticID: int) -> ReturnValue:
    # TODO: implement
    pass


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    # TODO: implement
    pass


def actorDidntPlayeInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    # TODO: implement
    pass


def getActorsRoleInMovie(actor_id: int, movie_name: str, movieYear: int) -> List[str]:
    # TODO: implement
    pass


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    # TODO: implement
    pass


def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    # TODO: implement
    pass


def averageActorRating(actorID: int) -> float:
    # TODO: implement
    pass


def bestPerformance(actor_id: int) -> Movie:
    # TODO: implement
    pass


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    # TODO: implement
    pass


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    conn = None
    grouped_revenues = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT T1.movieName, COALESCE(SUM(T1.production_revenue,0)) AS tot_revenue "
                        "FROM (Productions P1 RIGHT OUTER JOIN Movie M1 ON (P1.movieName = M1.movieName AND P1.movie_year = M1.movie_year)) AS T1"
                        "GROUP BY T1.movieName"
                        "ORDER BY movieName DESC;").format()
        _, result = conn.execute(query)
        conn.commit()
        if not result.isEmpty():
            for i in range(result.size()):
                grouped_revenues.append((result[i]['movieName'], result[i]['tot_revenue']))
    except Exception as e:
        print(e)
        grouped_revenues = []
    finally:
        conn.close()
    return grouped_revenues


def studioRevenueByYear() -> List[Tuple[int, int, int]]:
    conn = None
    grouped_revenues = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT T2.studioID, T2.movie_year, COALESCE(SUM(T2.production_revenue,0)) AS tot_revenue "
                        "FROM Productions AS T2"
                        "GROUP BY T2.studioID, T2.movie_year"
                        "ORDER BY T2.studioID DESC;").format()
        _, result = conn.execute(query)
        conn.commit()
        if not result.isEmpty():
            for i in range(result.size()):
                grouped_revenues.append((result[i]['studioID'], result[i]['movie_year'], result[i]['tot_revenue']))
    except Exception as e:
        print(e)
        grouped_revenues = []
    finally:
        conn.close()
    return grouped_revenues


def getFanCritics() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


def averageAgeByGenre() -> List[Tuple[str, float]]:
    # TODO: implement
    pass


def getExclusiveActors() -> List[Tuple[int, int]]:
    # TODO: implement
    pass

# GOOD LUCK!
