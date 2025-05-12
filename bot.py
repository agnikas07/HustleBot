import discord
from discord.ext import commands
from discord import app_commands
from discord import Interaction
import gspread
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, date
import logging
from collections import defaultdict

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(name)s: %(message)s')
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GOOGLE_SHEETS_CREDENTIALS_FILE = os.getenv("GOOGLE_SHEETS_CREDENTIALS_FILE")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME")
DATE_COLUMN_NAME = os.getenv("DATE_COLUMN_NAME")
NAME_COLUMN_NAME = os.getenv("NAME_COLUMN_NAME")
DIALS_COLUMN_NAME = os.getenv("DIALS_COLUMN_NAME")
DOORKNOCKS_COLUMN_NAME = os.getenv("DOORKNOCKS_COLUMN_NAME")
APPOINTMENTS_COLUMN_NAME = os.getenv("APPOINTMENTS_COLUMN_NAME")
PRESENTATIONS_COLUMN_NAME = os.getenv("PRESENTATIONS_COLUMN_NAME")
SHEET_DATE_FORMAT = os.getenv("SHEET_DATE_FORMAT")

# --- Input Validation ---
if not all([DISCORD_BOT_TOKEN, GOOGLE_SHEETS_CREDENTIALS_FILE, GOOGLE_SHEET_NAME, WORKSHEET_NAME,
            DATE_COLUMN_NAME, NAME_COLUMN_NAME, DIALS_COLUMN_NAME, DOORKNOCKS_COLUMN_NAME,
            APPOINTMENTS_COLUMN_NAME, PRESENTATIONS_COLUMN_NAME, SHEET_DATE_FORMAT]):
    logger.error("Missing one or more required environment variables in .env file. Exiting.")
    exit()

if not os.path.exists(GOOGLE_SHEETS_CREDENTIALS_FILE):
     logger.error(f"Google Sheets credentials file not found at path: {GOOGLE_SHEETS_CREDENTIALS_FILE}. Exiting.")
     exit()

# --- Google Sheets Setup ---
try:
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/drive.readonly'
    ]
    CREDS = Credentials.from_service_account_file(GOOGLE_SHEETS_CREDENTIALS_FILE, scopes=SCOPES)
    GC = gspread.authorize(CREDS)
    SPREADSHEET = GC.open(GOOGLE_SHEET_NAME)
    WORKSHEET = SPREADSHEET.worksheet(WORKSHEET_NAME)
    logger.info(f"Successfully connected to Google Sheet: '{GOOGLE_SHEET_NAME}' - Worksheet: '{WORKSHEET_NAME}'")
except gspread.exceptions.SpreadsheetNotFound:
    logger.error(f"Error: Spreadsheet '{GOOGLE_SHEET_NAME}' not found or bot lacks permissions.")
    exit()
except gspread.exceptions.WorksheetNotFound:
    logger.error(f"Error: Worksheet '{WORKSHEET_NAME}' not found in the spreadsheet.")
    exit()
except Exception as e:
    logger.error(f"An unexpected error occurred during Google Sheets setup: {e}")
    exit()

# --- Activity Mapping ---
ACTIVITY_MAP = {
    "dials": DIALS_COLUMN_NAME,
    "doorknocks": DOORKNOCKS_COLUMN_NAME,
    "appointments": APPOINTMENTS_COLUMN_NAME,
    "presentations": PRESENTATIONS_COLUMN_NAME,
}

# --- Create choices for the slash command argument ---
activity_choices = [
    app_commands.Choice(name=activity.capitalize(), value=activity)
    for activity in sorted(ACTIVITY_MAP.keys())
]

# --- Helper Functions ---
def get_current_week_dates():
    """Calculates the start (Monday) and end (Sunday) dates of the current week."""
    today = date.today()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=6)          # Sunday
    return start_of_week, end_of_week

def format_leaderboard(title, sorted_scores, start_date, end_date, target_column_name, top_n=9):
    """Formats the leaderboard data into a Discord Embed."""
    if not sorted_scores:
        embed = discord.Embed(title=title, description="No data found for this week.", color=discord.Color.orange())
        return embed

    embed = discord.Embed(title=title, color=discord.Color.gold())
    description = f"{target_column_name} from {start_date.strftime('%b %d')} - {end_date.strftime('%b %d')}\n\n"

    rank = 1
    for i, (name, score) in enumerate(sorted_scores[:top_n]):
        emoji = ""
        if rank == 1: emoji = "🥇 "
        elif rank == 2: emoji = "🥈 "
        elif rank == 3: emoji = "🥉 "
        else: emoji = f"{rank}️⃣. "

        embed.add_field(name=f"{emoji} {name}", value=f"{target_column_name} completed: **{score}**", inline=False)
        rank += 1

    if not description:
         description="No entries recorded for the top positions."

    embed.description = description
    embed.set_footer(text=f"Leaderboard generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return embed

# --- Discord Bot Setup ---
intents = discord.Intents.default()

bot = commands.Bot(command_prefix="!", intents=intents) # Prefix is now less relevant unless you keep old commands

@bot.event
async def on_ready():
    """Event handler for when the bot logs in and is ready."""
    logger.info(f'Logged in as {bot.user.name} (ID: {bot.user.id})')
    print('------')
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} application commands.")
        print(f"Synced {len(synced)} slash commands.")
    except Exception as e:
        logger.error(f"Failed to sync application commands: {e}")
        print(f"Failed to sync slash commands: {e}")

    print(f'Bot is ready and connected to Discord.')
    print(f'Use the command /leaderboard to get started.')
    print('------')


# --- Define the Slash Command ---
@bot.tree.command(name="leaderboard", description="Shows the weekly leaderboard for a specific activity.")
@app_commands.describe(activity="The activity type to get the leaderboard for")
@app_commands.choices(activity=activity_choices)
async def leaderboard_slash(interaction: Interaction, activity: str):
    """Slash command to fetch and display the weekly leaderboard."""
    await interaction.response.defer(thinking=True, ephemeral=False)

    if activity not in ACTIVITY_MAP:
        valid_activities = ", ".join(k.capitalize() for k in ACTIVITY_MAP.keys())
        await interaction.followup.send(f"Invalid activity type '{activity}'. Please choose from: {valid_activities}", ephemeral=True)
        logger.warning(f"Received invalid activity '{activity}' despite choices (User: {interaction.user.id}).")
        return

    target_column_name = ACTIVITY_MAP[activity]
    start_date, end_date = get_current_week_dates()
    logger.info(f"Processing /leaderboard for '{target_column_name}' (User: {interaction.user}) for week {start_date} to {end_date}")

    try:
        records = WORKSHEET.get_all_records()
        if not records:
             await interaction.followup.send("The Google Sheet appears to be empty.", ephemeral=True)
             logger.warning("Attempted to fetch data but the sheet is empty.")
             return

    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error: {e}")
        await interaction.followup.send("Error accessing Google Sheets. Please check permissions and API status.", ephemeral=True)
        return
    except Exception as e:
        logger.error(f"Unexpected error fetching data from Google Sheets: {e}")
        await interaction.followup.send("An unexpected error occurred while fetching data.", ephemeral=True)
        return

    user_scores = defaultdict(int)
    processed_rows = 0
    skipped_rows = 0
    log_flags = {
        'missing_date': False, 'missing_name': False, f'missing_{target_column_name}': False,
        'date_format': False, f'conversion_{target_column_name}': False
    }

    for record in records:
        try:
            if DATE_COLUMN_NAME not in record or NAME_COLUMN_NAME not in record or target_column_name not in record:
                 if not log_flags['missing_date'] and DATE_COLUMN_NAME not in record:
                      logger.warning(f"Row missing expected column: '{DATE_COLUMN_NAME}'. Further warnings suppressed. Row: {record}")
                      log_flags['missing_date'] = True
                 if not log_flags['missing_name'] and NAME_COLUMN_NAME not in record:
                     logger.warning(f"Row missing expected column: '{NAME_COLUMN_NAME}'. Further warnings suppressed. Row: {record}")
                     log_flags['missing_name'] = True
                 if not log_flags[f'missing_{target_column_name}'] and target_column_name not in record:
                      logger.warning(f"Row missing expected activity column: '{target_column_name}'. Further warnings suppressed. Row: {record}")
                      log_flags[f'missing_{target_column_name}'] = True
                 skipped_rows += 1
                 continue

            date_str = record.get(DATE_COLUMN_NAME, "")
            name = record.get(NAME_COLUMN_NAME, "").strip()
            activity_value_raw = record.get(target_column_name, "")

            if not date_str or not name:
                skipped_rows += 1
                continue

            try:
                record_date = datetime.strptime(str(date_str).strip(), SHEET_DATE_FORMAT).date()
            except ValueError:
                if not log_flags['date_format']:
                     logger.warning(f"Could not parse date '{date_str}' using format '{SHEET_DATE_FORMAT}'. Check .env and sheet data. Further warnings suppressed. Row: {record}")
                     log_flags['date_format'] = True
                skipped_rows += 1
                continue

            if not (start_date <= record_date <= end_date):
                continue

            try:
                activity_value = int(activity_value_raw) if str(activity_value_raw).strip() else 0
            except (ValueError, TypeError):
                 if not log_flags[f'conversion_{target_column_name}']:
                      logger.warning(f"Could not convert activity value '{activity_value_raw}' in column '{target_column_name}' to integer. Treating as 0. Further warnings suppressed. Record: {record}")
                      log_flags[f'conversion_{target_column_name}'] = True
                 activity_value = 0

            user_scores[name] += activity_value
            processed_rows += 1

        except Exception as e:
            logger.error(f"Error processing row: {record}. Error: {e}", exc_info=True)
            skipped_rows += 1
            continue

    logger.info(f"Data processing complete. Processed rows for week: {processed_rows}. Skipped rows (missing data/errors): {skipped_rows}. Unique users found: {len(user_scores)}")

    sorted_scores = sorted(user_scores.items(), key=lambda item: item[1], reverse=True)

    leaderboard_title = f"🏆 Weekly Leaderboard: {target_column_name.capitalize()} 🏆"
    embed = format_leaderboard(leaderboard_title, sorted_scores, start_date, end_date, target_column_name.capitalize())

    await interaction.followup.send(embed=embed)
    logger.info(f"Successfully generated and sent leaderboard for '{target_column_name}' via slash command.")

# --- Run the Bot ---
if __name__ == "__main__":
    if DISCORD_BOT_TOKEN:
        bot.run(DISCORD_BOT_TOKEN)
    else:
        logger.error("Discord bot token is not configured. Set DISCORD_BOT_TOKEN in the .env file.")