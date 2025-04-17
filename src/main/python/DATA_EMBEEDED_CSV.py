import pandas as pd
import psycopg2
import os
import sys

def import_players_from_csv():
    try:
        print("Cricket CSV Data Importer")
        print("========================")
        
        # Get CSV file paths
        batting_csv = os.path.join(os.getcwd(), 'all_season_batting_card.csv')
        bowling_csv = os.path.join(os.getcwd(), 'all_season_bowling_card.csv')
        
        # Verify files exist
        if not os.path.exists(batting_csv):
            print(f"Error: File not found: {batting_csv}")
            sys.exit(1)
            
        if not os.path.exists(bowling_csv):
            print(f"Error: File not found: {bowling_csv}")
            sys.exit(1)
        
        # Connect to database
        print("Connecting to database...")
        conn = psycopg2.connect(
            dbname="cricket_db",
            user="postgres",
            password="cricket123",
            host="localhost"
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        # Import batting data
        print("Importing batting data...")
        batting_data = pd.read_csv(batting_csv)
        print(f"Found {len(batting_data)} batting records")
        
        # Get unique batsmen
        unique_batsmen = batting_data['name'].unique()
        print(f"Found {len(unique_batsmen)} unique batsmen")
        
        # Process and insert batting data
        for batsman in unique_batsmen:
            player_data = batting_data[batting_data['name'] == batsman]
            
            # Calculate career stats and convert to native Python types
            career_runs = float(player_data['runs'].sum())
            total_balls_faced = float(player_data['ballsFaced'].sum())
            strike_rate = float((career_runs / total_balls_faced * 100) if total_balls_faced > 0 else 0)
            highest_score = float(player_data['runs'].max())
            innings_count = int(len(player_data))
            
            # Check if player already exists
            cur.execute(
                "SELECT player_name FROM batch_batting_analysis WHERE player_name = %s",
                (str(batsman),)
            )
            
            if cur.fetchone():
                # Update existing player
                cur.execute("""
                    UPDATE batch_batting_analysis 
                    SET career_runs = %s, 
                        avg_strike_rate = %s, 
                        total_balls_faced = %s, 
                        highest_score = %s, 
                        innings_count = %s 
                    WHERE player_name = %s
                """, (career_runs, strike_rate, total_balls_faced, highest_score, innings_count, str(batsman)))
            else:
                # Insert new player
                cur.execute("""
                    INSERT INTO batch_batting_analysis 
                    (player_name, career_runs, avg_strike_rate, total_balls_faced, highest_score, innings_count) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (str(batsman), career_runs, strike_rate, total_balls_faced, highest_score, innings_count))
        
        # Import bowling data
        print("Importing bowling data...")
        bowling_data = pd.read_csv(bowling_csv)
        print(f"Found {len(bowling_data)} bowling records")
        
        # Get unique bowlers
        unique_bowlers = bowling_data['name'].unique()
        print(f"Found {len(unique_bowlers)} unique bowlers")
        
        # Process and insert bowling data
        for bowler in unique_bowlers:
            player_data = bowling_data[bowling_data['name'] == bowler]
            
            # Calculate career stats and convert to native Python types
            career_wickets = float(player_data['wickets'].sum())
            runs_conceded = float(player_data['conceded'].sum())
            
            # Calculate economy rate - ensure we don't divide by zero
            overs = float(player_data['overs'].sum())
            economy_rate = float((runs_conceded / overs) if overs > 0 else 0)
            
            best_bowling = float(player_data['wickets'].max())
            spells_bowled = int(len(player_data))
            
            # Check if player already exists
            cur.execute(
                "SELECT bowler_name FROM batch_bowling_analysis WHERE bowler_name = %s",
                (str(bowler),)
            )
            
            if cur.fetchone():
                # Update existing player
                cur.execute("""
                    UPDATE batch_bowling_analysis 
                    SET career_wickets = %s, 
                        avg_economy = %s, 
                        total_runs_conceded = %s, 
                        best_bowling_figures = %s, 
                        spells_bowled = %s 
                    WHERE bowler_name = %s
                """, (career_wickets, economy_rate, runs_conceded, best_bowling, spells_bowled, str(bowler)))
            else:
                # Insert new player
                cur.execute("""
                    INSERT INTO batch_bowling_analysis 
                    (bowler_name, career_wickets, avg_economy, total_runs_conceded, best_bowling_figures, spells_bowled) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (str(bowler), career_wickets, economy_rate, runs_conceded, best_bowling, spells_bowled))
        
        # Verify data was imported correctly
        cur.execute("SELECT COUNT(DISTINCT player_name) FROM batch_batting_analysis")
        batsmen_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT bowler_name) FROM batch_bowling_analysis")
        bowlers_count = cur.fetchone()[0]
        
        print("\nImport completed successfully!")
        print(f"Batsmen in database: {batsmen_count}")
        print(f"Bowlers in database: {bowlers_count}")
        
        # Close database connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    import_players_from_csv()
