import os
import pandas as pd
import numpy as np
import datetime as dt
import gspread 
import dash
from dash import dcc, html, dash_table
import json
from google.oauth2.service_account import Credentials

# Load service account JSON from the Render Secret File
with open("/etc/secrets/RENDER_SECRET") as f:
    service_account_info = json.load(f)

credentials = Credentials.from_service_account_info(
    service_account_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(credentials)

pd.set_option('display.expand_frame_repr', False)  # Prevent line wrapping

def main():
    contestbeta = gc.open("2025 Playoffs - Wild Card (Responses)")
    pickinput = contestbeta.worksheet("Form Responses 1")
    picksraw = pd.DataFrame(pickinput.get_all_records())

    # Rename columns, drop email and notes
    picks = picksraw.set_axis(
        ['timestamp', 'email', 'name', 'afc1', 'afc1con', 'afc2', 'afc2con', 'afc3', 'afc3con',
         'nfc1', 'nfc1con', 'nfc2', 'nfc2con', 'nfc3', 'nfc3con', 'notes'],
        axis=1
    ).drop(columns=['email', 'notes'])

    # Convert timestamp to datetime for sorting
    picks['timestamp'] = pd.to_datetime(picks['timestamp'])

    # Define the shared deadlines (first kickoff for each day)
    shared_deadlines = {
        'saturday': pd.Timestamp('2025-01-11 13:30:00'),  # First Saturday game
        'sunday': pd.Timestamp('2025-01-12 10:00:00')     # First Sunday game (applies to Monday too)
    }

    # Map each game to the correct deadline
    game_deadlines = {
        'afc1': shared_deadlines['saturday'],
        'afc2': shared_deadlines['saturday'],
        'afc3': shared_deadlines['sunday'],
        'nfc1': shared_deadlines['sunday'],
        'nfc2': shared_deadlines['sunday'],
        'nfc3': shared_deadlines['sunday']  # Monday game but same as Sunday deadline
    }

    # Pivot to long format, preserving multiple picks
    picks_long = pd.melt(picks, 
                         id_vars=['timestamp', 'name'], 
                         value_vars=['afc1', 'afc2', 'afc3', 'nfc1', 'nfc2', 'nfc3'],
                         var_name='game', 
                         value_name='pick')

    conf_long = pd.melt(picks,
                        id_vars=['timestamp', 'name'], 
                        value_vars=['afc1con', 'afc2con', 'afc3con', 'nfc1con', 'nfc2con', 'nfc3con'],
                        var_name='game', 
                        value_name='confidence')

    # Clean confidence column names to match picks
    conf_long['game'] = conf_long['game'].str.replace('con', '')

    # Merge picks with confidence
    df_merged = pd.merge(picks_long, conf_long, on=['timestamp', 'name', 'game'])

    # Assign deadline based on the game
    df_merged['deadline'] = df_merged['game'].map(game_deadlines)

    # Flag late picks
    df_merged['late'] = df_merged['timestamp'] > df_merged['deadline']

    # Sort 
    df_merged.sort_values(by=['game', 'name', 'timestamp'], inplace=True)

    # Filter to valid picks
    valid_picks = df_merged[df_merged['timestamp'] <= df_merged['deadline']]
    valid_picks = valid_picks[valid_picks['pick'] != '']
    valid_picks = valid_picks.sort_values(by=['timestamp']).groupby(['name', 'game']).tail(1)
    valid_picks = valid_picks.dropna(subset=['confidence'])
    valid_picks = valid_picks[valid_picks['confidence'].apply(lambda x: isinstance(x, int))]

    # Define the game order
    game_order = {'afc1': 1, 'afc2': 2, 'afc3': 3, 'nfc1': 4, 'nfc2': 5, 'nfc3': 6}
    valid_picks['game_order'] = valid_picks['game'].map(game_order)
    valid_picks['dupval'] = None

    # Zero out duplicates
    used_conf = {}
    for idx, row in valid_picks.iterrows():
        player = row['name']
        conf = row['confidence']

        if player not in used_conf:
            used_conf[player] = set()

        if conf in used_conf[player]:
            valid_picks.at[idx, 'confidence'] = 0
            valid_picks.at[idx, 'dupval'] = conf
        else:
            used_conf[player].add(conf)

    valid_picks = valid_picks.drop(columns=['game_order'])

    # Confidence tracking
    valid_picks['adjusted_conf'] = valid_picks.apply(
        lambda row: row['confidence'] if row['confidence'] != 0 else row['dupval'], axis=1
    )
    player_confidence = (
        valid_picks.groupby('name')['adjusted_conf']
        .apply(lambda x: sorted(list(x)))
        .reset_index(name='entered_conf')
    )

    def zero_duplicates(conf_list):
        seen = set()
        effective = []
        for conf in conf_list:
            if conf in seen:
                effective.append(0)
            else:
                effective.append(conf)
                seen.add(conf)
        return effective

    player_confidence['effective_conf'] = player_confidence['entered_conf'].apply(zero_duplicates)

    def calculate_remaining_conf(effective_conf):
        all_values = set(range(1, 14))
        remaining = sorted(all_values - set(effective_conf))
        for conf in effective_conf:
            if conf == 0 and remaining:
                remaining.pop(0)
        return remaining

    player_confidence['remaining_conf'] = player_confidence['effective_conf'].apply(calculate_remaining_conf)

    # Import game results
    playoffs_wc = gc.open("2025 Playoffs - Wild Card (Responses)")
    wc_lines_scores = playoffs_wc.worksheet("lines_scores")
    wc_results = pd.DataFrame(wc_lines_scores.get_all_records())

    # Score picks
    if 'winner_ATS' not in valid_picks.columns:
        valid_picks = valid_picks.merge(wc_results[['game', 'winner_ATS', 'complete']], on='game', how='left')
    valid_picks['correct'] = (valid_picks['pick'] == valid_picks['winner_ATS']).astype(int)
    valid_picks['points_won'] = valid_picks['confidence'] * (
        (valid_picks['winner_ATS'] == 'Push') * 0.5 + valid_picks['correct']
    )

    # Mark points_won as None for unscored games
    valid_picks['points_won'] = valid_picks.apply(
        lambda row: row['points_won'] if row['complete'] else None,  # Set points_won to None for unscored games
        axis=1
    )

    # Filter only completed games (optional: for scoring-related analysis only)
    valid_picks = valid_picks[valid_picks['complete'] == 1]

    # Prepare data for Dash display
    game_info = wc_results[['game', 'away', 'home', 'homeline', 'awaypts', 'homepts', 'winner_ATS']]
    
    # Player Score Summary: Sorted in descending order by points scored
    player_scores = valid_picks.groupby('name')['points_won'].sum().reset_index(name='total_points')
    player_scores = player_scores.sort_values(by='total_points', ascending=False).reset_index(drop=True)

    player_scores = player_scores.merge(
        player_confidence[['name', 'remaining_conf']],
        on='name',
        how='left'
    )
    player_scores['remaining_conf'] = player_scores['remaining_conf'].apply(lambda x: ', '.join(map(str, x)))
    # Reformat Player Picks and Points: One row per player, games as columns
    # Create a new column combining pick and confidence
    valid_picks['pick_conf'] = valid_picks['pick'] + " (" + valid_picks['confidence'].astype(str) + ")"

    # Pivot the table: Players as rows, games as columns
    picks_results_pivot = valid_picks.pivot(index='name', columns='game', values='pick_conf').reset_index()

    # Add Points Scored column
    picks_results_pivot = picks_results_pivot.merge(
        player_scores[['name', 'total_points']],
        on='name',
        how='left'
    )

    # Rename columns for clarity
    picks_results_pivot = picks_results_pivot.rename(columns={'total_points': 'Points Scored'})

    return game_info, player_scores, picks_results_pivot

# Entry point
if __name__ == '__main__':
    # Prepare data
    game_info, player_scores, picks_results_pivot = main()

    # Initialize Dash app
    app = dash.Dash(__name__)
    app.layout = html.Div([
        html.H1("NFL Playoff Contest Results"),
        html.H2("Game Results"),
        dash_table.DataTable(
            data=game_info.to_dict('records'),
            columns=[{"name": i, "id": i} for i in game_info.columns],
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'center', 'padding': '10px'},
            style_header={'backgroundColor': 'lightblue', 'fontWeight': 'bold'}
        ),
        html.H2("Player Picks and Points"),
        dash_table.DataTable(
            data=picks_results_pivot.to_dict('records'),
            columns=[{"name": i, "id": i} for i in picks_results_pivot.columns],
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'center', 'padding': '10px'},
            style_header={'backgroundColor': 'lightblue', 'fontWeight': 'bold'}
        ),
        dash_table.DataTable(
            data=picks_results_pivot.to_dict('records'),
            columns=[
                {"name": "Player", "id": "name"},
                {"name": "Game", "id": "game"},
                {"name": "Pick", "id": "pick"},
                {"name": "Confidence", "id": "confidence"},
                {"name": "Points Won", "id": "points_won"}
            ],
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'center', 'padding': '10px'},
            style_header={'backgroundColor': 'lightblue', 'fontWeight': 'bold'}
        )

    ], style={'fontFamily': 'Arial, sans-serif'})

    # Run the Dash app
    port = int(os.environ.get('PORT', 8000))  # Default to 8000 if PORT is not set
    app.run_server(debug=True, host='0.0.0.0', port=port)