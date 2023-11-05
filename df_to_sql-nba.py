import pandas as pd
import mysql.connector

mydb = mysql.connector.connect(
    host="localhost", user="root", password="123456", database="basquete"
)

teams = pd.read_csv("teams.csv")
players = pd.read_csv("players.csv")
games = pd.read_csv("games.csv")
games_details = pd.read_csv("games_details.csv", dtype={"NICKNAME": object})

mycursor = mydb.cursor()

sql = "INSERT INTO times (Nome, time_df_id) VALUES (%s, %s)"
val = list(map(lambda x: (x[0], x[1]), teams[["NICKNAME", "TEAM_ID"]].to_numpy()))
mycursor.executemany(sql, val)

mycursor.execute("SELECT time_df_id, TimeId FROM times")
timesID = pd.DataFrame(
    map(lambda x: [x[0], x[1]], mycursor.fetchall()), columns=["TEAM_ID", "bd_id_times"]
)

sql = "INSERT INTO jogador (Nome, idTime, jogador_df_id) VALUES (%s, %s, %s)"
val = list(
    map(
        lambda x: (x[0], x[1], x[2]),
        players[players["SEASON"] == 2019]
        .merge(teams)[["PLAYER_NAME", "TEAM_ID", "PLAYER_ID"]]
        .merge(timesID)[["PLAYER_NAME", "bd_id_times", "PLAYER_ID"]]
        .to_numpy(),
    )
)
mycursor.executemany(sql, val)

mycursor.execute("SELECT jogador_df_id, JogadorId FROM jogador")
jogadorID = pd.DataFrame(
    map(lambda x: [x[0], x[1]], mycursor.fetchall()),
    columns=["PLAYER_ID", "bd_id_jogador"],
)

games["TEAM_ID"] = games["HOME_TEAM_ID"]
sql = "INSERT INTO jogo (Local, Data, jogo_df_id) VALUES (%s, %s, %s)"
val = list(
    map(
        lambda x: (x[0], x[1], x[2]),
        games[games["SEASON"] == 2019]
        .merge(teams)[["CITY", "GAME_DATE_EST", "GAME_ID"]]
        .to_numpy(),
    )
)
mycursor.executemany(sql, val)

mycursor.execute("SELECT jogo_df_id, JogoId FROM jogo")
jogoID = pd.DataFrame(
    map(lambda x: [x[0], x[1]], mycursor.fetchall()), columns=["GAME_ID", "bd_id_jogo"]
)

sql = "INSERT INTO jogador_jogo (JogoId, JogadorId) VALUES (%s, %s)"
val = list(
    map(
        lambda x: (int(x[0]), int(x[1])),
        games_details[games_details["START_POSITION"].notna()]
        .merge(jogoID, how="right")
        .merge(jogadorID[["PLAYER_ID", "bd_id_jogador"]])[
            ["bd_id_jogo", "bd_id_jogador"]
        ]
        .to_numpy(),
    )
)
mycursor.executemany(sql, val)

jogoBdIdsDic = (
    jogoID[["GAME_ID", "bd_id_jogo"]].set_index("GAME_ID").to_dict()["bd_id_jogo"]
)
timeBdIdsDic = (
    timesID[["TEAM_ID", "bd_id_times"]].set_index("TEAM_ID").to_dict()["bd_id_times"]
)
agrupandoTimeJogoDic = (
    games_details.groupby(["GAME_ID", "TEAM_ID"])[["PF"]].agg("sum").to_dict()["PF"]
)
dadosJogosDic = (
    games.merge(jogoID, how="right")[
        ["GAME_ID", "PTS_home", "PTS_away", "HOME_TEAM_ID", "VISITOR_TEAM_ID"]
    ]
    .set_index("GAME_ID")
    .to_dict()
)

sql = "INSERT INTO time_jogo (TimeId, JogoId, casa_visitante, pts_a_favor, Faltas) VALUES (%s, %s, %s, %s, %s)"
val = []
for i in jogoBdIdsDic.keys():
    val.append(
        (
            timeBdIdsDic[dadosJogosDic["HOME_TEAM_ID"][i]],
            jogoBdIdsDic[i],
            "casa",
            dadosJogosDic["PTS_home"][i],
            agrupandoTimeJogoDic[(i, dadosJogosDic["HOME_TEAM_ID"][i])],
        )
    )
    val.append(
        (
            timeBdIdsDic[dadosJogosDic["VISITOR_TEAM_ID"][i]],
            jogoBdIdsDic[i],
            "visitante",
            dadosJogosDic["PTS_away"][i],
            agrupandoTimeJogoDic[(i, dadosJogosDic["HOME_TEAM_ID"][i])],
        )
    )
mycursor.executemany(sql, val)


mydb.commit()
