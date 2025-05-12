# HustleBot
Discord bot that examines Google Sheets data (insurance sales activity like dials, doorknocks, etc.) and compiles data into a weekly leaderboard when prompted to by discord channel users.

You will need to set a .env file with the following data:

""""
DISCORD_BOT_TOKEN=

GOOGLE_SHEETS_CREDENTIALS_FILE=
GOOGLE_SHEET_NAME=
WORKSHEET_NAME=

DATE_COLUMN_NAME=Date
NAME_COLUMN_NAME=Name
DIALS_COLUMN_NAME=Dials
DOORKNOCKS_COLUMN_NAME=Doorknocks
APPOINTMENTS_COLUMN_NAME=Appointments
PRESENTATIONS_COLUMN_NAME=Presentations

# You can replace the column names with the corresponding column names on your Google Sheet

SHEET_DATE_FORMAT=%Y-%m-%d
""""

You will also need to post your Google Service Account JSON file in the same directory as your .env and bot.py file.