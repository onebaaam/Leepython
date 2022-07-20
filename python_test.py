import cx_Oracle
import pandas as pd
import requests
from tqdm import tqdm
import random
from random import sample
import time
import plotly.express as px
from matplotlib import font_manager, rc
font_path = "C:/Windows/Fonts/gulim.ttc"
font = font_manager.FontProperties(fname=font_path).get_name()
rc('font',family=font)

dsn = cx_Oracle.makedsn('localhost', 1521, 'xe')
api_key = 'RGAPI-2c78b969-be1a-4191-bf81-bac30de49ad5'

def db_open():
    global db
    global cursor
    db = cx_Oracle.connect(user='ICIA', password='1111', dsn=dsn)
    cursor = db.cursor()
    print('open!')

def sql_execute(q):
    global db
    global cursor
    try:
        if 'select' in q:
            df = pd.read_sql(sql = q, con=db)
            return df
        cursor.execute(q)
        return 'success'
    except Exception as e:
        print(e)

def db_close():
    global db
    global cursor
    try:
        db.commit()
        cursor.close()
        db.close()
        return 'close'
    except Exception as e:
        print(e)


def get_puuid(SN):
    url =  'https://kr.api.riotgames.com/lol/summoner/v4/summoners/by-name/'+SN+'?api_key='+api_key
    res = requests.get(url).json()
    puuid = res['puuid']
    return puuid

def get_matchid(pId, games):
    url = 'https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/' + pId + '/ids?queue=420&type=ranked&start=0&count=' + str(games) + '&api_key=' + api_key
    res = requests.get(url).json()
    return res

def get_matches_timelines(mId):
    lst = []
    for matchid in tqdm(mId):
        url = 'https://asia.api.riotgames.com/lol/match/v5/matches/' + matchid + '?api_key=' + api_key
        res1 = requests.get(url).json()
        url = 'https://asia.api.riotgames.com/lol/match/v5/matches/' + matchid + '/timeline?api_key=' + api_key
        res2 = requests.get(url).json()
        lst.append([matchid, res1, res2])


    return lst


def get_rawData(num):
    tier_list = ['BRONZE','SILVER','GOLD','PLATINUM','DIAMOND']
    division_list = ['I','II','III','IV']
    page = random.randrange(1,10)

    for i in range(num):
        for tier in tier_list:
            try:
                for division in division_list:
                    print(tier+division+'진행')

                    try:
                        df_creater = []
                        lst = []
                        url = 'https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5/'+tier+'/'+division+'?page='+str(page)+'&api_key='+api_key
                        res = requests.get(url).json()
                        lst += sample(res,3)

                        print('get SummonerName')
                        summonerName_lst = list(map(lambda x : x['summonerName'], lst))
                    except:
                        print('get SumonerName 예외')
                        continue
                    puuid_lst = []
                    for SN in tqdm(summonerName_lst):
                        try:
                            puuid_lst.append(get_puuid(SN))
                        except:
                            print(SN+'예외')
                            continue
                    matchid_lst = []

                    for pId in tqdm(puuid_lst):
                        try:
                            matchid_lst.extend(get_matchid(pId,2))
                        except:
                            print('get matchid 예외')
                            continue
                    time.sleep(40)
                    print(tier+division+' making raw_data...')
                    match_timeline_lst = get_matches_timelines(matchid_lst)
                    df_creater.extend(match_timeline_lst)

                    df = pd.DataFrame(df_creater, columns=['gameId', 'matches', 'timeline'])
                    df2 = get_match_timeline_df(df)
                    print('get_match_timeline_df success!')

                    db_open()
                    tqdm.pandas()
                    df2.progress_apply(lambda x: insert_matches_timeline(x), axis=1)[0]
                    db_close()
                    print(tier + division+ ' insert complete! 60초 대기')
                    time.sleep(60)
            except:
                print('tier load exception')



    return df2



def get_match_timeline_df(df):

    df_creater = []
    print('매치 데이터 생성중....')
    for i in tqdm (range(len(df))):
        try:
            for j in range(9):
                tmp = []
                tmp.append(df.iloc[i].gameId)
                tmp.append(df.iloc[i].matches['info']['gameDuration'])
                tmp.append(df.iloc[i].matches['info']['gameVersion'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerName'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['summonerLevel'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['participantId'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['championName'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['champExperience'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['teamPosition'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['teamId'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['win'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['kills'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['deaths'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['assists'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageDealtToChampions'])
                tmp.append(df.iloc[i].matches['info']['participants'][j]['totalDamageTaken'])
                for k in range(5,21):
                    try:
                        tmp.append(df.iloc[i].timeline['info']['frames'][k]['participantFrames'][str(j+1)]['totalGold'])
                    except:
                        tmp.append(0)
                df_creater.append(tmp)
        except:
            print('매치 데이터 예외')
            continue
    columns = ['gameId','gameDuration','gameVersion','summonerName','summonerLevel','participantId','championName','champExperience',
           'teamPosition','teamId','win','kills','deaths','assists','totalDamageDealtToChampions','totalDamageTaken','g_5','g_6','g_7','g_8','g_9','g_10','g_11','g_12','g_13','g_14','g_15','g_16','g_17',
           'g_18','g_19','g_20']
    df = pd.DataFrame(df_creater,columns = columns).drop_duplicates()
    print('complete!')
    return df

def insert_matches_timeline(row):
    # lambda를 이용해서 progress_apply를 통해 insert할 구문 만들기
    query = (
             f'MERGE INTO TEST_DATA USING DUAL ON(gameId=\'{row.gameId}\' and participantId={row.participantId}) '
             f'WHEN NOT MATCHED THEN '
             f'insert (gameId, gameDuration, gameVersion, summonerName, summonerLevel, participantId,'
             f'championName, champExperience, teamPosition, teamId, win, kills, deaths, assists,'
             f'totalDamageDealtToChampions, totalDamageTaken, g_5, g_6, g_7, g_8, g_9, g_10, g_11, g_12 ,g_13,g_14,'
             f'g_15, g_16, g_17, g_18, g_19, g_20)'
             f'values(\'{row.gameId}\',{row.gameDuration}, \'{row.gameVersion}\', \'{row.summonerName}\','
             f'{row.summonerLevel}, {row.participantId},\'{row.championName}\',{row.champExperience},'
             f'\'{row.teamPosition}\', {row.teamId}, \'{row.win}\', {row. kills}, {row.deaths}, {row.assists},'
             f'{row.totalDamageDealtToChampions},{row.totalDamageTaken},{row.g_5},{row.g_6},{row.g_7},{row.g_8},'
             f'{row.g_9},{row.g_10},{row.g_11},{row.g_12},{row.g_13},{row.g_14},{row.g_15},{row.g_16},{row.g_17},'
             f'{row.g_18},{row.g_19},{row.g_20})'
            )
    sql_execute(query)
    return query

def create_table():
    query = (
    f'CREATE TABLE TEST_DATA (gameId varchar(30), gameDuration number(20), gameVersion varchar(30), summonerName varchar(50), summonerLevel number(20), participantId number(5),'
    f'championName varchar(30), champExperience number(30), teamPosition varchar(20), teamId  number(20), win varchar(20), kills number(20), deaths number(20), assists number(20),'
    f' totalDamageDealtToChampions number(20), totalDamageTaken number(20)'
    f', g_5 number(20), g_6 number(20), g_7 number(20), g_8 number(20), g_9 number(20), g_10 number(20), g_11 number(20), g_12 number(20)'
    f',g_13 number(20), g_14 number(20), g_15 number(20), g_16 number(20), g_17 number(20), g_18 number(20), g_19 number(20), g_20 number(20),'
    f'constraint pt_pk primary key(gameId,participantId))'
    )
    db_open()
    sql_execute(query)
    db_close()
    print('테이블 생성')

def select_table():
    db_open()
    df = sql_execute('select * from TEST_DATA')
    db_close()
    print('테이블 셀렉')
    return df

def show_avg_deal(df):

    deal_df = df[['CHAMPIONNAME', 'TOTALDAMAGEDEALTTOCHAMPIONS', 'TOTALDAMAGETAKEN']].groupby((['CHAMPIONNAME'])).mean().round(2)
    fig = px.scatter(deal_df, x="TOTALDAMAGEDEALTTOCHAMPIONS", y="TOTALDAMAGETAKEN", color=deal_df.index,
                     text=deal_df.index)
    fig.update_traces(textposition='top center')
    fig.update_layout(title_text='이원빈_챔피언별 딜량 및 피해량 그래프')
    fig.show()

    return fig

def show_avg_position_15g(df):
    avg15g = df[['TEAMPOSITION', 'G_15']].groupby((['TEAMPOSITION'])).mean().round(2)
    fig = px.bar(avg15g, x=avg15g.index, y="G_15", color=avg15g.index)
    fig.update_layout(title_text='이원빈_포지션(라인)별 15분 평균 골드')
    fig.show()

    return fig

