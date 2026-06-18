import csv
import psycopg2
import logging
import time
from psycopg2.extras import execute_values
from datetime import datetime

db_config={
    "host":"localhost",
    "port":"5432",
    "database":"cdc_db",
    "user":"Aman",
    "password":"panchal"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("ipl_loader")


def to_int(value):
    if not value or value.strip()=="":
        return None
    try:
        return int(float(value))
    except(ValueError,TypeError):
        return None
    
def to_float(value):
    if not value or value.strip()=="":
        return None
    try:
        return float(value)
    except(ValueError,TypeError):
        return None


def to_string(value):
    if not value or value.strip()=="":
        return None
    return str(value).strip()


def to_date(value):
    if not value or value.strip()=="":
        return None
    for i in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(value.strip(),i).date()
        except:
            continue
    log.warning(f"Date fail, NULL: {value}")
    return None


def load_csv_match_id(deliveries_path):
    group={}
    with open(deliveries_path,newline="",encoding="utf-8") as f:
        reader=csv.DictReader(f)
        for row in reader:
            ipl_id=to_int(row.get("match_id"))
            group.setdefault(ipl_id,[]).append(row)
    return group

MATCH_INSERT_SQL = """
    INSERT INTO matches (
        id, season, city, match_date, match_type, player_of_match, venue,
        team1, team2, toss_winner, toss_decision, winner, result,
        result_margin, target_runs, target_overs, super_over, method,
        umpire1, umpire2
    )
    VALUES (
        %(id)s, %(season)s, %(city)s, %(match_date)s, %(match_type)s,
        %(player_of_match)s, %(venue)s, %(team1)s, %(team2)s,
        %(toss_winner)s, %(toss_decision)s, %(winner)s, %(result)s,
        %(result_margin)s, %(target_runs)s, %(target_overs)s,
        %(super_over)s, %(method)s, %(umpire1)s, %(umpire2)s
    )
    ON CONFLICT (id) DO NOTHING;
"""
 
DELIVERY_INSERT_SQL = """
    INSERT INTO deliveries (
        match_id, inning, batting_team, bowling_team, "over", ball,
        batter, bowler, non_striker, batsman_runs, extra_runs, total_runs,
        extras_type, is_wicket, player_dismissed, dismissal_kind, fielder
    )
    VALUES %s
    ON CONFLICT (match_id, inning, "over", ball) DO NOTHING;
"""
 
 
def insert_match(cur, row):
    params = {
        "id": to_int(row.get("id")),
        "season": to_string(row.get("season")),
        "city": to_string(row.get("city")),
        "match_date": to_date(row.get("date") or row.get("match_date")),
        "match_type": to_string(row.get("match_type")),
        "player_of_match": to_string(row.get("player_of_match")),
        "venue": to_string(row.get("venue")),
        "team1": to_string(row.get("team1")),
        "team2": to_string(row.get("team2")),
        "toss_winner": to_string(row.get("toss_winner")),
        "toss_decision": to_string(row.get("toss_decision")),
        "winner": to_string(row.get("winner")),
        "result": to_string(row.get("result")),
        "result_margin": to_int(row.get("result_margin")),
        "target_runs": to_int(row.get("target_runs")),
        "target_overs": to_float(row.get("target_overs")),
        "super_over": to_string(row.get("super_over")),
        "method": to_string(row.get("method")),
        "umpire1": to_string(row.get("umpire1")),
        "umpire2": to_string(row.get("umpire2")),
    }
    cur.execute(MATCH_INSERT_SQL, params)
 
 
def insert_deliveries_chunk(cur, chunk_rows):
    values = []
    for row in chunk_rows:
        values.append((
            to_int(row.get("match_id")),
            to_int(row.get("inning")),
            to_string(row.get("batting_team")),
            to_string(row.get("bowling_team")),
            to_int(row.get("over")),
            to_int(row.get("ball")),
            to_string(row.get("batter")),
            to_string(row.get("bowler")),
            to_string(row.get("non_striker")),
            to_int(row.get("batsman_runs")),
            to_int(row.get("extra_runs")),
            to_int(row.get("total_runs")),
            to_string(row.get("extras_type")),
            to_int(row.get("is_wicket")),
            to_string(row.get("player_dismissed")),
            to_string(row.get("dismissal_kind")),
            to_string(row.get("fielder")),
        ))
    execute_values(cur, DELIVERY_INSERT_SQL, values)

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def main():
    log.info("Deliveries CSV ko match_id ke hisaab se group kar rahe hain...")
    deliveries_by_match=load_csv_match_id("data/deliveries.csv")
    
    conn=psycopg2.connect(**db_config)
    conn.autocommit=False
    cur=conn.cursor()
    
    total_match=0
    total_deliveries=0
    current_season = None
    
    try:
        with open("data/matches.csv",newline="",encoding="utf-8") as m:
            reader=csv.DictReader(m)
            for match_row in reader:
                match_id=to_int(match_row.get("id"))
                if match_id is None:
                    log.warning("match_id missing , skip")
                    continue
                season = to_string(match_row.get("season"))
                
                if season != current_season:
                    log.info(f"Season {season} start")
                    current_season = season
                    
                    
                try:
                    insert_match(cur,match_row)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    log.error(f"Match {match_id} insert fail: {e}")
                    continue
                
                time.sleep(0.3)
                
                match_deliveries=deliveries_by_match.get(match_id,[])
                for chunk in chunked(match_deliveries,25):
                    try:
                        insert_deliveries_chunk(cur,chunk)
                        conn.commit()
                        total_deliveries += len(chunk)
                    except Exception as e:
                        conn.rollback()
                        log.error(f"Match {match_id} deliveries chunk FAIL: {e}")
                        continue
                    time.sleep(0.15)
                    
                total_match+=1
                if total_match % 5 ==0:
                    log.info(
                        f"{total_match} matches done | "
                        f"{total_deliveries} deliveries done | "
                    )
                time.sleep(1)
                
    finally:
        cur.close()
        conn.close()

    log.info(
        f"DONE -> {total_match} matches, {total_deliveries} deliveries "
    )

if __name__ == "__main__":
    main()