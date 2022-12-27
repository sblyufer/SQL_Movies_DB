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
                              "criticID INTEGER PRIMARY KEY,"
                              "critic_name TEXT NOT NULL,"
                              "CHECK (criticID > 0));"

                              "CREATE TABLE Movies("
                              "movieName TEXT NOT NULL, "
                              "movie_year INTEGER NOT NULL CHECK (movie_year >= 1895),"
                              "movie_genre TEXT NOT NULL CHECK (movie_genre = 'Drama' OR  movie_genre = 'Action' OR movie_genre = 'Comedy' OR movie_genre = 'Horror'), "
                              "CONSTRAINT Movies_key PRIMARY KEY (movieName, movie_year)); "

                              "CREATE TABLE Actors("
                              "actorID INTEGER PRIMARY KEY,"
                              "actor_name TEXT NOT NULL, "
                              "actor_age INTEGER NOT NULL,"
                              "actor_height INTEGER NOT NULL,"
                              "CHECK (actorID > 0), CHECK (actor_age > 0), CHECK (actor_height > 0));"

                              "CREATE TABLE Studios("
                              "studioID INTEGER PRIMARY KEY, "
                              "studio_name TEXT NOT NULL, "
                              "CHECK (studioID > 0)); "

                              "CREATE TABLE Productions("
                              "production_budget INTEGER NOT NULL CHECK (production_budget >= 0), "
                              "production_revenue INTEGER NOT NULL CHECK (production_revenue >=0), "
                              "studioID INTEGER NOT NULL,"
                              "movieName TEXT NOT NULL, "
                              "movie_year INTEGER NOT NULL CHECK (movie_year >= 1895),"
                              "FOREIGN KEY (studioID) REFERENCES Studios(studioID) ON DELETE CASCADE, "
                              "FOREIGN KEY (movieName, movie_year) REFERENCES Movies(movieName, movie_year) ON DELETE CASCADE, "
                              "CONSTRAINT Productions_key PRIMARY KEY (movieName, movie_year)); "

                              "CREATE TABLE Reviews("
                              "review_rating INTEGER NOT NULL CHECK (review_rating > 0), CHECK (review_rating < 6), "
                              "movieName TEXT NOT NULL, "
                              "movie_year INTEGER NOT NULL CHECK (movie_year >= 1895),"
                              "criticID INTEGER,"
                              "FOREIGN KEY (criticID) REFERENCES Critics(criticID) ON DELETE CASCADE, "
                              "FOREIGN KEY (movieName, movie_year) REFERENCES Movies(movieName, movie_year) ON DELETE CASCADE, "
                              "CONSTRAINT Reviews_key PRIMARY KEY (movieName, movie_year, criticID)); "

                              "CREATE TABLE Roles("
                              "roleName TEXT NOT NULL, "
                              "movieName TEXT NOT NULL, "
                              "movie_year INTEGER NOT NULL CHECK (movie_year >= 1895),"
                              "actorID INTEGER NOT NULL,"
                              "FOREIGN KEY (actorID) REFERENCES Actors(actorID) ON DELETE CASCADE, "
                              "FOREIGN KEY (movieName, movie_year) REFERENCES Movies(movieName, movie_year) ON DELETE CASCADE, "
                              "CONSTRAINT Roles_key PRIMARY KEY (movieName, movie_year, actorID)); "

                              "CREATE TABLE ActingJobs("
                              "job_salary INTEGER NOT NULL CHECK (job_salary > 0), "
                              "movieName TEXT NOT NULL, "
                              "movie_year INTEGER NOT NULL CHECK (movie_year >= 1895),"
                              "actorID INTEGER NOT NULL,"
                              "FOREIGN KEY (actorID) REFERENCES Actors(actorID) ON DELETE CASCADE, "
                              "FOREIGN KEY (movieName,movie_year) REFERENCES Movies(movieName,movie_year) ON DELETE CASCADE,"
                              "CONSTRAINT Jobs_key PRIMARY KEY (movieName, movie_year, actorID));"

                              "CREATE VIEW CriticToStudio AS "
                              "SELECT V1.studioID, COUNT(*) AS count1 "
                              "FROM (SELECT R1.criticID, P1.studioID"
                              "      FROM Productions P1 INNER JOIN Reviews R1"
                              "      ON P1.movieName = R1.movieName AND P1.movie_year = R1.movie_year) AS V1 "
                              "GROUP BY V1.criticID, V1.studioID;"

                              "CREATE VIEW StudioFilms AS "
                              "SELECT V2.studioID, COUNT(*) AS count2 "
                              "FROM (SELECT P2.studioID, P2.movieName, P2.movie_year "
                              "      FROM Productions P2) AS V2 "
                              "GROUP BY V2.studioID;"

                              "CREATE VIEW ActorsToGenre AS "
                              "SELECT M.movie_genre, AVG(A.actorID)"
                              "FROM Movies M "
                              "INNER JOIN ActingJobs AJ "
                              "ON M.movieName = AJ.movieName "
                              "AND M.movie_year = AJ.movie_year "
                              "INNER JOIN Actors A "
                              "ON A.actorID = AJ.actorID "
                              "GROUP BY M.movie_genre;").format()

        conn.execute(transaction)
        conn.commit()
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

                              "DROP TABLE IF EXISTS Critics CASCADE; "

                              "DROP TABLE IF EXISTS Movies CASCADE;"

                              "DROP TABLE IF EXISTS Actors CASCADE; "

                              "DROP TABLE IF EXISTS Studios CASCADE; "

                              "DROP VIEW IF EXISTS StudioFilms CASCADE; "

                              "DROP VIEW IF EXISTS CriticsToStudio CASCADE; "

                              "DROP TABLE IF EXISTS Roles CASCADE;"

                              "DROP TABLE IF EXISTS ActingJobs CASCADE; "

                              "DROP TABLE IF EXISTS Reviews CASCADE; "

                              "DROP TABLE IF EXISTS Productions CASCADE; "

                              "DROP TABLE IF EXISTS ActorsToGenre CASCADE; "

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
                        "WHERE (movieName = {id1} AND movie_year = {id2});").format(
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
                            "FROM Movies "
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
            movie.setGenre(str(result[0]['movie_genre']))
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


def criticRatedMovie(movieName: str, movieYear: int, critic_id: int, rating: int) -> ReturnValue:
    result = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Reviews(movieName,movie_year,criticID,review_rating) "
                        "VALUES({movieName}, {movieYear}, {critic_id}, {rating})") \
            .format(movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear),
                    critic_id=sql.Literal(critic_id), rating=sql.Literal(rating))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result


def criticDidntRateMovie(movieName: str, movieYear: int, critic_id: int) -> ReturnValue:
    conn = None
    rows_effected= 0, result=ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE "
                        "FROM Reviews "
                        "WHERE movieName={movieName} AND movie_year={movieYear}  AND criticID={critic_id}"). \
            format(movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear), critic_id=sql.Literal(critic_id))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return result


def actorPlayedInMovie(movieName: str, movieYear: int, actorID: int, salary: int, roles: List[str]) -> ReturnValue:
    conn = None
    rows_effected= 0, result=ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO ActingJobs (job_salary, movieName, movie_year, actorID) VALUES ({job_salary},{movieName},{movieYear},{actorID})")\
            .format(job_salary=sql.Literal(salary), movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear), actorID=sql.Literal(actorID))
        rows_effected, _ = conn.execute(query)
        for role in roles:
            conn.execute("INSERT INTO Roles (roleName, movieName, movie_year, actorID) VALUES ({role},{movieName},{movieYear},{actorID})")\
                .format(role=sql.Literal(role), movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear), actorID=sql.Literal(actorID))
            rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return result


def actorDidntPlayInMovie(movieName: str, movieYear: int, actorID: int) -> ReturnValue:
    conn = None
    rows_effected= 0, result=ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE "
                        "FROM Roles "
                        "WHERE movieName={movieName} AND movie_year={movieYear}  AND actorID={actorID}"). \
            format(movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear), actorID=sql.Literal(actorID))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return result


def studioProducedMovie(studioID: int, movieName: str, movieYear: int, budget: int, revenue: int) -> ReturnValue:
    result = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Productions(production_budget,production_revenue,studioID,movieName,movie_year) "
                        "VALUES({_budget}, {_revenue}, {_studioID}, {_movieName}, {_movieYear})") \
            .format(_budget=sql.Literal(budget), _revenue=sql.Literal(revenue),
                    _studioID=sql.Literal(studioID), _movieName=sql.Literal(movieName), _movieYear=sql.Literal(movieYear))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        return result



def studioDidntProduceMovie(studioID: int, movieName: str, movieYear: int) -> ReturnValue:
    conn = None
    rows_effected= 0, result=ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE "
                        "FROM Productions "
                        "WHERE movieName={movieName} AND movie_year={movieYear} AND studioID={studioID}").format(movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear), studioID=sql.Literal(studioID))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        result = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        result = ReturnValue.NOT_EXISTS
    except Exception as e:
        result = ReturnValue.ERROR
    finally:
        conn.close()
        if rows_effected == 0:
            return ReturnValue.NOT_EXISTS
        return result


# ---------------------------------- BASIC API: ----------------------------------
def averageRating(movieName: str, movieYear: int) -> float:
    conn = None
    output = 0
    rows_effected, result = 0
    try:
        query = sql.SQL("SELECT AVG(review_rating) "
                        "FROM Reviews "
                        "WHERE movieName={movieName} AND movie_year={movieYear}").\
            format(movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear))
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(query)
        # rows_effected is the number of rows received by the SELECT
        for row in result.rows:
            for val in row:
                output = val
        if output is None:
            output = 0
    except DatabaseException.ConnectionInvalid as e:
        output = 0
    except DatabaseException.NOT_NULL_VIOLATION as e:
        output = 0
    except DatabaseException.CHECK_VIOLATION as e:
        output = 0
    except DatabaseException.UNIQUE_VIOLATION as e:
        output = 0
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        output = 0
    except Exception as e:
        output = 0
    finally:
        conn.close()
        return output


def averageActorRating(actorID: int) -> float:
    conn = None
    result = None
    avg = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT AVG(review_rating) FROM Reviews INNER JOIN Roles ON Reviews.movieName = Roles.movieName AND Reviews.movie_year = Roles.movie_year WHERE Roles.actorID = {actorID}"
            .format(
                actorID=sql.Literal(actorID))

        rows_effected, result = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        avg = 0
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        avg = 0
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        avg = 0
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        avg = 0
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        avg = 0
    except Exception as e:
        conn.rollback()
        avg = 0
    finally:
        if result:
            record = result.fetchone()
            if record is None:
                return 0

        conn.close()
        return record[0];


def bestPerformance(actor_id: int) -> Movie:
    conn = None
    rows_effected= 0, result=ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(    "SELECT m.movieName, m.movie_year, AVG(r.review_rating) as avg_rating "
                            "FROM Movies m INNER JOIN Roles r "
                            "ON m.movieName = r.movieName AND m.movie_year = r.movie_year "
                            "WHERE r.actorID = {actorID} "
                            "GROUP BY m.movieName, m.movie_year "
                            "ORDER BY avg_rating DESC, m.movie_year ASC, m.movieName ASC "
                            "LIMIT 1").\
            format(movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear), actorID=sql.Literal(actorID))
        _, result = conn.execute(query)
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


def stageCrewBudget(movieName: str, movieYear: int) -> int:
    conn = Connector.DBConnector()
    cur = conn.cursor()

    query = sql.SQL(
        "SELECT production_budget, SUM(job_salary) FROM Productions P INNER JOIN ActingJobs A ON P.movieName = A.movieName AND P.movie_year = A.movie_year WHERE P.movieName = {movieName} AND P.movie_year = {movie_year}"). \
        format(movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear)
    cur.execute(query)
    row = cur.fetchone()
    if row == None:
        return -1
    else:
    return row[0] - row[1]


def overlyInvestedInMovie(movie_name: str, movie_year: int, actor_id: int) -> bool:
    conn = None
    result=None
    avg=0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * "
                        "FROM (SELECT COALESCE(COUNT(review_rating),0) AS cnt1 "
                            "FROM Roles"
                            "WHERE movieName={movieName} AND movie_year={movieYear} AND actorID={actorID}) AS R1 "
                            "WHERE R1.cnt1 >= (SELECT COUNT(review_rating)*0.5 "
                                            "FROM Roles"
                                            "WHERE movieName={movieName} AND movie_year={movieYear}" ) \
                            .format(movieName=sql.Literal(movieName), movieYear=sql.Literal(movieYear), actorID=sql.Literal(actorID))

        rows_effected, result = conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        avg = False
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        avg = False
    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        avg = False
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        avg = False
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        avg = False
    except Exception as e:
        conn.rollback()
        avg = False
    finally:
        if result:
            if not result.isEmpty():
                avg = TRUE

        conn.close()
        return avg


# ---------------------------------- ADVANCED API: ----------------------------------


def franchiseRevenue() -> List[Tuple[str, int]]:
    conn = None
    grouped_revenues = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT "
                        "M1.movieName, COALESCE(SUM(P1.production_revenue),0) AS tot_revenue "
                        "FROM (Productions P1 RIGHT OUTER JOIN Movies M1 "
                        "ON (P1.movieName = M1.movieName AND P1.movie_year = M1.movie_year)) "
                        "GROUP BY M1.movieName "
                        "ORDER BY M1.movieName DESC ;").format()
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
        query = sql.SQL("SELECT studioID, movie_year, SUM(production_revenue) AS tot_revenue "
                        "FROM Productions "
                        "GROUP BY studioID, movie_year "
                        "ORDER BY studioID DESC;").format()
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
    conn = None
    #TODO: fix no studios
    fan_critics = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT criticID, studioID"
                        "FROM CriticToStudio"
                        "WHERE CriticToStudio.critic_unique = (SELECT studio_unique FROM StudioFilms WHERE StudioFilms.studioID = studioID)"
                        "ORDER BY criticID DESC, studioID DESC;").format()
        _, result = conn.execute(query)
        conn.commit()
        if not result.isEmpty():
            for i in range(result.size()):
                fan_critics.append((result[i]['criticID'], result[i]['studioID']))
    except Exception as e:
        print(e)
        fan_critics = []
    finally:
        conn.close()
    return fan_critics


def averageAgeByGenre() -> List[Tuple[str, float]]:
    conn = None
    avg_by_genre = []
    try:
        conn = Connector.DBConnector()
        # query = sql.SQL("SELECT "
        #                 "M.movie_genre, AVG(DISTINCT A.actorID) as avg "
        #                 "FROM Movies M "
        #                 "INNER JOIN ActingJobs AJ "
        #                 "ON M.movieName = AJ.movieName "
        #                 "AND M.movie_year = AJ.movie_year"
        #                 "INNER JOIN Actors A "
        #                 "ON A.actorID = AJ.actorID AS "
        #                 "GROUP BY M.movie_genre ;").format()
        query = sql.SQL("SELECT *"
                        "FROM ActorsToGenre;").format()
        _, result = conn.execute(query)
        conn.commit()
        if not result.isEmpty():
            for i in range(result.size()):
                avg_by_genre.append((result[i]['movie_genre'], result[i]['avg']))
    except Exception as e:
        print(e)
        avg_by_genre = []
    finally:
        conn.close()
    return avg_by_genre


def getExclusiveActors() -> List[Tuple[int, int]]:
    # TODO: implement
    pass


# cur.execute("""SELECT A.actor_id, P.studio_id
#                 FROM Actors A
#                 INNER JOIN Roles R
#                 ON A.actor_id = R.actor_id
#                 INNER JOIN Productions P
#                 ON P.movieName = R.movieName
#                 AND P.movie_year = R.movie_year
#                 GROUP BY A.actor_id, P.studio_id
#                 HAVING COUNT(DISTINCT P.studio_id) = 1
#                 ORDER BY A.actor_id DESC""")
# rows = cur.fetchall()
# return rows

# GOOD LUCK!
