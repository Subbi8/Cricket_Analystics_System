import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from matplotlib import cm
from matplotlib.ticker import MaxNLocator
import numpy as np

class EnhancedReportGenerator:
    def __init__(self):
        try:
            self.conn = psycopg2.connect(
                dbname="cricket_db",
                user="postgres",
                password="cricket123",
                host="localhost"
            )
            self.cur = self.conn.cursor()
            self.report_dir = os.path.join(os.getcwd(), 'reports')
            os.makedirs(self.report_dir, exist_ok=True)
            
            # Set up style for beautiful visualizations
            plt.style.use('ggplot')
            sns.set_style("whitegrid")
            sns.set_context("talk")
            
        except psycopg2.Error as e:
            print(f"Database connection failed: {e}")
            sys.exit(1)

    def get_players(self):
        """Get players as a flat list with type information."""
        players = []
        
        # Get batsmen
        self.cur.execute("SELECT DISTINCT player_name FROM batch_batting_analysis")
        for row in self.cur.fetchall():
            players.append({"name": row[0], "type": "batsman"})
        
        # Get bowlers
        self.cur.execute("SELECT DISTINCT bowler_name FROM batch_bowling_analysis")
        for row in self.cur.fetchall():
            # Check if this player is already in the list as a batsman
            player_exists = False
            for p in players:
                if p["name"] == row[0]:
                    p["type"] = "all-rounder"  # Update to all-rounder if they both bat and bowl
                    player_exists = True
                    break
            
            if not player_exists:
                players.append({"name": row[0], "type": "bowler"})
        
        return players

    def generate_batting_report(self, selected_players=None):
        try:
            query = """
                SELECT 
                    player_name,
                    career_runs,
                    avg_strike_rate,
                    highest_score,
                    innings_count
                FROM batch_batting_analysis
            """
            
            if selected_players:
                # Only include batsmen and all-rounders
                batsmen = [p["name"] for p in selected_players if p["type"] in ["batsman", "all-rounder"]]
                
                if batsmen:
                    placeholders = ','.join(['%s'] * len(batsmen))
                    query += f" WHERE player_name IN ({placeholders})"
                    query += " ORDER BY career_runs DESC"
                    self.cur.execute(query, batsmen)
                else:
                    # If no batsmen selected, don't generate batting report
                    print("No batsmen found in selection.")
                    return None, None
            else:
                query += " ORDER BY career_runs DESC LIMIT 10"  # Limit to top 10 for better visualization
                self.cur.execute(query)
            
            batting_data = pd.DataFrame(
                self.cur.fetchall(),
                columns=['Player', 'Career Runs', 'Avg Strike Rate', 'Highest Score', 'Innings']
            )
            
            if batting_data.empty:
                print("No batting data found for selected players")
                return None, None

            # Create a more attractive plot for batting performance
            self._generate_batting_visualization(batting_data)
            
            # Save to CSV
            csv_path = os.path.join(self.report_dir, 'batting_statistics.csv')
            batting_data.to_csv(csv_path, index=False)
            
            return os.path.join(self.report_dir, 'batting_performance.png'), csv_path
            
        except Exception as e:
            print(f"Error generating batting report: {e}")
            import traceback
            traceback.print_exc()
            return None, None

    def _generate_batting_visualization(self, batting_data):
        """Create multiple visualizations for batting performance and save to file."""
        fig = plt.figure(figsize=(18, 12))
        fig.suptitle('Batting Performance Analysis', fontsize=24, fontweight='bold', y=0.98)
        
        # Create a layout with 2 rows and 2 columns
        ax1 = plt.subplot2grid((2, 2), (0, 0))  # Top left
        ax2 = plt.subplot2grid((2, 2), (0, 1))  # Top right
        ax3 = plt.subplot2grid((2, 2), (1, 0), colspan=2)  # Bottom (spans both columns)
        
        # 1. Career Runs - Bar chart
        colors = cm.viridis(np.linspace(0, 1, len(batting_data)))
        ax1.bar(batting_data['Player'], batting_data['Career Runs'], color=colors)
        ax1.set_title('Career Runs by Player', fontsize=16)
        ax1.set_ylabel('Runs')
        ax1.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for i, v in enumerate(batting_data['Career Runs']):
            ax1.text(i, v + 50, f"{int(v)}", ha='center', fontweight='bold')
        
        # 2. Strike Rate - Horizontal bar chart
        batting_data = batting_data.sort_values('Avg Strike Rate')
        ax2.barh(batting_data['Player'], batting_data['Avg Strike Rate'], color=cm.plasma(np.linspace(0, 1, len(batting_data))))
        ax2.set_title('Average Strike Rate', fontsize=16)
        ax2.set_xlabel('Strike Rate')
        
        # Add value labels on bars
        for i, v in enumerate(batting_data['Avg Strike Rate']):
            ax2.text(v + 2, i, f"{v:.2f}", va='center', fontweight='bold')
        
        # 3. Runs vs Innings - Scatter plot with size indicating strike rate
        # Create colors based on strike rate
        norm = plt.Normalize(batting_data['Avg Strike Rate'].min(), batting_data['Avg Strike Rate'].max())
        colors = cm.coolwarm(norm(batting_data['Avg Strike Rate']))
        
        scatter = ax3.scatter(
            batting_data['Innings'],
            batting_data['Career Runs'],
            s=batting_data['Highest Score'] * 3,  # Size based on highest score
            c=batting_data['Avg Strike Rate'],  # Color based on strike rate
            cmap='coolwarm',
            alpha=0.8
        )
        
        # Add player names as annotations
        for i, player in enumerate(batting_data['Player']):
            ax3.annotate(
                player,
                (batting_data['Innings'].iloc[i], batting_data['Career Runs'].iloc[i]),
                xytext=(5, 5),
                textcoords='offset points',
                fontsize=8
            )
        
        ax3.set_title('Career Runs vs Innings Played (Size: Highest Score, Color: Strike Rate)', fontsize=16)
        ax3.set_xlabel('Innings Played')
        ax3.set_ylabel('Career Runs')
        
        # Add a colorbar for the strike rate
        cbar = plt.colorbar(scatter, ax=ax3)
        cbar.set_label('Strike Rate')
        
        plt.tight_layout(rect=[0, 0, 1, 0.96])  # Adjust for the main title
        
        # Save visualization
        plot_path = os.path.join(self.report_dir, 'batting_performance.png')
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()

    def generate_bowling_report(self, selected_players=None):
        try:
            query = """
                SELECT 
                    bowler_name,
                    career_wickets,
                    avg_economy,
                    best_bowling_figures,
                    spells_bowled,
                    total_runs_conceded
                FROM batch_bowling_analysis
            """
            
            if selected_players:
                # Only include bowlers and all-rounders
                bowlers = [p["name"] for p in selected_players if p["type"] in ["bowler", "all-rounder"]]
                
                if bowlers:
                    placeholders = ','.join(['%s'] * len(bowlers))
                    query += f" WHERE bowler_name IN ({placeholders})"
                    query += " ORDER BY career_wickets DESC"
                    self.cur.execute(query, bowlers)
                else:
                    # If no bowlers selected, don't generate bowling report
                    print("No bowlers found in selection.")
                    return None, None
            else:
                query += " ORDER BY career_wickets DESC LIMIT 10"  # Limit to top 10 for better visualization
                self.cur.execute(query)
            
            bowling_data = pd.DataFrame(
                self.cur.fetchall(),
                columns=['Bowler', 'Wickets', 'Economy Rate', 'Best Figures', 'Spells', 'Runs Conceded']
            )
            
            if bowling_data.empty:
                print("No bowling data found for selected players")
                return None, None

            # Create a more attractive plot for bowling performance
            self._generate_bowling_visualization(bowling_data)
            
            # Save to CSV
            csv_path = os.path.join(self.report_dir, 'bowling_statistics.csv')
            bowling_data.to_csv(csv_path, index=False)
            
            return os.path.join(self.report_dir, 'bowling_performance.png'), csv_path
            
        except Exception as e:
            print(f"Error generating bowling report: {e}")
            import traceback
            traceback.print_exc()
            return None, None

    def _generate_bowling_visualization(self, bowling_data):
        """Create multiple visualizations for bowling performance and save to file."""
        fig = plt.figure(figsize=(18, 12))
        fig.suptitle('Bowling Performance Analysis', fontsize=24, fontweight='bold', y=0.98)
        
        # Create a layout with 2 rows and 2 columns
        ax1 = plt.subplot2grid((2, 2), (0, 0))  # Top left
        ax2 = plt.subplot2grid((2, 2), (0, 1))  # Top right
        ax3 = plt.subplot2grid((2, 2), (1, 0), colspan=2)  # Bottom (spans both columns)
        
        # 1. Wickets - Bar chart
        colors = cm.viridis(np.linspace(0, 1, len(bowling_data)))
        ax1.bar(bowling_data['Bowler'], bowling_data['Wickets'], color=colors)
        ax1.set_title('Career Wickets by Bowler', fontsize=16)
        ax1.set_ylabel('Wickets')
        ax1.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for i, v in enumerate(bowling_data['Wickets']):
            ax1.text(i, v + 1, f"{int(v)}", ha='center', fontweight='bold')
        
        # 2. Economy Rate - Horizontal bar chart (sorted)
        bowling_data_sorted = bowling_data.sort_values('Economy Rate')
        ax2.barh(bowling_data_sorted['Bowler'], bowling_data_sorted['Economy Rate'], 
                color=cm.plasma(np.linspace(0, 1, len(bowling_data_sorted))))
        ax2.set_title('Economy Rate', fontsize=16)
        ax2.set_xlabel('Runs per Over')
        
        # Add value labels on bars
        for i, v in enumerate(bowling_data_sorted['Economy Rate']):
            ax2.text(v + 0.1, i, f"{v:.2f}", va='center', fontweight='bold')
        
        # 3. Wicket Efficiency (Wickets per spell) vs Economy - Scatter plot
        bowling_data['Wicket Efficiency'] = bowling_data['Wickets'] / bowling_data['Spells']
        
        scatter = ax3.scatter(
            bowling_data['Economy Rate'],
            bowling_data['Wicket Efficiency'],
            s=bowling_data['Best Figures'] * 30,  # Size based on best figures
            c=bowling_data['Wickets'],  # Color based on total wickets
            cmap='YlOrRd',
            alpha=0.8
        )
        
        # Add player names as annotations
        for i, bowler in enumerate(bowling_data['Bowler']):
            ax3.annotate(
                bowler,
                (bowling_data['Economy Rate'].iloc[i], bowling_data['Wicket Efficiency'].iloc[i]),
                xytext=(5, 5),
                textcoords='offset points',
                fontsize=8
            )
        
        ax3.set_title('Wicket Efficiency vs Economy Rate (Size: Best Figures, Color: Total Wickets)', fontsize=16)
        ax3.set_xlabel('Economy Rate (Lower is better)')
        ax3.set_ylabel('Wickets per Spell (Higher is better)')
        
        # Add a colorbar for the wickets
        cbar = plt.colorbar(scatter, ax=ax3)
        cbar.set_label('Total Wickets')
        
        # Add a reference line for wicket efficiency
        avg_efficiency = bowling_data['Wicket Efficiency'].mean()
        ax3.axhline(y=avg_efficiency, color='gray', linestyle='--', alpha=0.7)
        ax3.text(bowling_data['Economy Rate'].max(), avg_efficiency, 
                 f'Avg: {avg_efficiency:.2f}', va='center', ha='right')
        
        plt.tight_layout(rect=[0, 0, 1, 0.96])  # Adjust for the main title
        
        # Save visualization
        plot_path = os.path.join(self.report_dir, 'bowling_performance.png')
        plt.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close()

    def __del__(self):
        try:
            if hasattr(self, 'cur') and self.cur:
                self.cur.close()
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
        except Exception as e:
            print(f"Error closing database connections: {e}")


def main():
    try:
        print("==== ENHANCED CRICKET PERFORMANCE REPORT GENERATOR ====")
        generator = EnhancedReportGenerator()
        
        # Get players
        players = generator.get_players()
        
        # Group players by type for better display
        batsmen = [p for p in players if p["type"] == "batsman"]
        bowlers = [p for p in players if p["type"] == "bowler"]
        all_rounders = [p for p in players if p["type"] == "all-rounder"]
        
        # Display players with indices
        print(f"\nAvailable Players ({len(players)} total):")
        
        # Print batsmen
        print(f"\nBatsmen ({len(batsmen)}):")
        for i, player in enumerate(batsmen, 1):
            print(f"{i}. {player['name']}")
        
        # Print bowlers
        start_idx = len(batsmen) + 1
        print(f"\nBowlers ({len(bowlers)}):")
        for i, player in enumerate(bowlers, start_idx):
            print(f"{i}. {player['name']}")
        
        # Print all-rounders
        start_idx = len(batsmen) + len(bowlers) + 1
        print(f"\nAll-Rounders ({len(all_rounders)}):")
        for i, player in enumerate(all_rounders, start_idx):
            print(f"{i}. {player['name']}")
        
        # Create a mapping from indices to players
        player_map = {}
        idx = 1
        
        # Add batsmen to the map
        for player in batsmen:
            player_map[idx] = player
            idx += 1
        
        # Add bowlers to the map
        for player in bowlers:
            player_map[idx] = player
            idx += 1
        
        # Add all-rounders to the map
        for player in all_rounders:
            player_map[idx] = player
            idx += 1
        
        # Get user selection
        print("\nSelect players (Enter numbers separated by commas, or press Enter for all players)")
        selection = input("Your selection: ").strip()
        
        selected_players = None
        if selection:
            try:
                # Parse user input
                indices = [int(x.strip()) for x in selection.split(",")]
                selected_players = []
                
                # Process selection
                print("\nSelected players:")
                for idx in indices:
                    if idx in player_map:
                        player = player_map[idx]
                        selected_players.append(player)
                        print(f"- {player['name']} ({player['type']})")
                    else:
                        print(f"Invalid index: {idx}")
                
                if not selected_players:
                    print("No valid players selected. Generating reports for all players.")
                    selected_players = None
            except ValueError:
                print("Invalid input. Generating reports for top players.")
        
        # Generate reports
        batting_plot, batting_csv = generator.generate_batting_report(selected_players)
        if batting_plot and batting_csv:
            print(f"\nBatting reports generated:")
            print(f"- Performance plot: {batting_plot}")
            print(f"- Statistics CSV: {batting_csv}")
        
        bowling_plot, bowling_csv = generator.generate_bowling_report(selected_players)
        if bowling_plot and bowling_csv:
            print(f"\nBowling reports generated:")
            print(f"- Performance plot: {bowling_plot}")
            print(f"- Statistics CSV: {bowling_csv}")
        
        print("\nReport generation completed!")
        print("\nOpen the 'reports' folder to view the generated reports and visualizations.")
        
    except Exception as e:
        print(f"An error occurred during report generation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
