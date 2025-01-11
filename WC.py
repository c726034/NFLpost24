#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import datetime as dt
import gspread 
import requests
import dash
from dash import dcc, html, dash_table
import os
import json
from google.oauth2.service_account import Credentials

pd.set_option('display.expand_frame_repr', False)  # Prevent line wrapping


def main():
    
    # Load service account JSON from the Render Secret File
    with open("/etc/secrets/RENDER_SECRET") as f:  # Adjust to match your secret file name
        service_account_info = json.load(f)

    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(credentials)

    
    # Load data from Google Sheets
    contestbeta = gc.open("2024 Playoffs - Wild Card (Responses)")
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

    # Process picks data
    picks_long = pd.melt(
        picks,
        id_vars=['timestamp', 'name'],
        value_vars=['afc1', 'afc2', 'afc3', 'nfc1', 'nfc2', 'nfc3'],
        var_name='game',
        value_name='pick'
    )
    conf_long = pd.melt(
        picks,
        id_vars=['timestamp', 'name'],
        value_vars=['afc1con', 'afc2con', 'afc3con', 'nfc1con', 'nfc2con', 'nfc3con'],
        var_name='game',
        value_name='confidence'
    )
    conf_long['game'] = conf_long['game'].str.replace('con', '')

    df_merged = pd.merge(picks_long, conf_long, on=['timestamp', 'name', 'game'])
    df_merged['deadline'] = df_merged['game'].map(game_deadlines)
    df_merged['late'] = df_merged['timestamp'] > df_merged['deadline']
    df_merged.sort_values(by=['game', 'name', 'timestamp'], inplace=True)

    # Additional processing, scoring, and Dash setup would follow here...

    # Initialize Dash App
    app = dash.Dash(__name__)
    app.layout = html.Div([
        html.H1("NFL Playoff Contest Results"),
        html.H2("More layout components to follow..."),
    ])

    app.run_server(debug=False, host='0.0.0.0', port=8000)


if __name__ == '__main__':
    main()
