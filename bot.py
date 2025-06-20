import discord
from discord.ext import commands, tasks
from discord import app_commands
from discord import Interaction
import gspread
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, time as dt_time 
import logging
from collections import defaultdict
import pytz 

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
RECRUITING_INTERVIEWS_COLUMN_NAME = os.getenv("RECRUITING_INTERVIEWS_COLUMN_NAME")
SHEET_DATE_FORMAT = os.getenv("SHEET_DATE_FORMAT")
LEADERBOARD_CHANNEL_ID = os.getenv("LEADERBOARD_CHANNEL_ID")
REMINDER_CHANNEL_ID = os.getenv("REMINDER_CHANNEL_ID") 
REMINDER_LINK = os.getenv("REMINDER_LINK")

# --- Input Validation ---
if not all([DISCORD_BOT_TOKEN, GOOGLE_SHEETS_CREDENTIALS_FILE, GOOGLE_SHEET_NAME, WORKSHEET_NAME,
            DATE_COLUMN_NAME, NAME_COLUMN_NAME, DIALS_COLUMN_NAME, DOORKNOCKS_COLUMN_NAME,
            APPOINTMENTS_COLUMN_NAME, PRESENTATIONS_COLUMN_NAME, RECRUITING_INTERVIEWS_COLUMN_NAME, SHEET_DATE_FORMAT, 
            LEADERBOARD_CHANNEL_ID, REMINDER_CHANNEL_ID]): 
    logger.error("Missing one or more required environment variables in .env file (including LEADERBOARD_CHANNEL_ID, REMINDER_CHANNEL_ID). Exiting.")
    exit()

if not os.path.exists(GOOGLE_SHEETS_CREDENTIALS_FILE):
     logger.error(f"Google Sheets credentials file not found at path: {GOOGLE_SHEETS_CREDENTIALS_FILE}. Exiting.")
     exit()

try:
    LEADERBOARD_CHANNEL_ID = int(LEADERBOARD_CHANNEL_ID)
    REMINDER_CHANNEL_ID = int(REMINDER_CHANNEL_ID) 
except ValueError:
    logger.error("LEADERBOARD_CHANNEL_ID and REMINDER_CHANNEL_ID in .env file must be valid integers. Exiting.")
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
    "recruiting_interviews": RECRUITING_INTERVIEWS_COLUMN_NAME,
}

# --- Create choices for the slash command argument ---
activity_choices = [
    app_commands.Choice(name=key.replace("_", " ").title(), value=key)
    for key in sorted(ACTIVITY_MAP.keys())
]
activity_choices = sorted(activity_choices, key=lambda c: c.name) 


# --- Helper Functions ---
def get_current_week_dates():
    now_est = datetime.now(EST)
    today_est = now_est.date()
    start_of_week = today_est - timedelta(days=today_est.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week, end_of_week

def format_leaderboard(title, sorted_scores, start_date, end_date, unit_name_display, top_n=9):
    filtered_scores = [(name, score) for name, score in sorted_scores if score > 0]
    if not filtered_scores:
        return None  # No data to post

    embed = discord.Embed(title=title, color=discord.Color.gold())
    description = f"{unit_name_display} from {start_date.strftime('%b %d')} - {end_date.strftime('%b %d')}\nSubmit your numbers here: {REMINDER_LINK}\n\n"

    rank = 1
    for i, (name, score) in enumerate(filtered_scores[:top_n]):
        emoji = ""
        if rank == 1: 
            emoji = "🥇" 
        elif rank == 2: 
            emoji = "🥈"
        elif rank == 3: 
            emoji = "🥉"
        else: 
            emoji = f"{rank}."

        embed.add_field(name=f"{emoji}{name}", value=f"{unit_name_display} completed: **{score}**", inline=False)
        rank += 1

    embed.description = description
    embed.set_footer(text=f"Leaderboard generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return embed

async def fetch_and_format_activity_leaderboard(activity_key: str, worksheet, start_date, end_date):
    target_column_name = ACTIVITY_MAP[activity_key]
    unit_name_display = activity_key.replace("_", " ").title()
    logger.info(f"Generating leaderboard for '{target_column_name}' for week {start_date} to {end_date}")

    try:
        records = worksheet.get_all_records()
        if not records:
            logger.warning(f"Google Sheet is empty when generating leaderboard for {activity_key}.")
            return discord.Embed(title=f"🏆 Weekly Leaderboard: {unit_name_display} 🏆", description="The Google Sheet appears to be empty.", color=discord.Color.orange())
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error for {activity_key}: {e}")
        return discord.Embed(title=f"🏆 Weekly Leaderboard: {unit_name_display} 🏆", description="Error accessing Google Sheets.", color=discord.Color.red())
    except Exception as e:
        logger.error(f"Unexpected error fetching data from Google Sheets for {activity_key}: {e}")
        return discord.Embed(title=f"🏆 Weekly Leaderboard: {unit_name_display} 🏆", description="An unexpected error occurred while fetching data.", color=discord.Color.red())

    user_scores = defaultdict(int)
    processed_rows = 0
    skipped_rows = 0
    first_warning_flags = {'missing_column': False, 'date_parse': False, 'value_conversion': False}

    for record_num, record in enumerate(records, 1):
        try:
            if not all(col in record for col in [DATE_COLUMN_NAME, NAME_COLUMN_NAME, target_column_name]):
                if not first_warning_flags['missing_column']:
                    logger.warning(f"Row {record_num} missing columns for {activity_key}. Row: {record}")
                    first_warning_flags['missing_column'] = True
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
                if not first_warning_flags['date_parse']:
                    logger.warning(f"Could not parse date '{date_str}' in row {record_num} for {activity_key}. Row: {record}")
                    first_warning_flags['date_parse'] = True
                skipped_rows += 1
                continue

            if not (start_date <= record_date <= end_date):
                continue

            try:
                activity_value_str = str(activity_value_raw).strip()
                activity_value = int(activity_value_str) if activity_value_str else 0
            except (ValueError, TypeError):
                if not first_warning_flags['value_conversion']:
                    logger.warning(f"Could not convert value '{activity_value_raw}' for {activity_key} (row {record_num}). Record: {record}")
                    first_warning_flags['value_conversion'] = True
                activity_value = 0

            user_scores[name] += activity_value
            processed_rows += 1
        except Exception as e:
            logger.error(f"Error processing row {record_num} for {activity_key}: {record}. Error: {e}", exc_info=True)
            skipped_rows +=1
            continue
    
    logger.info(f"Data processing for {activity_key} complete. Processed: {processed_rows}, Skipped: {skipped_rows}, Users: {len(user_scores)}")
    sorted_scores = sorted(user_scores.items(), key=lambda item: item[1], reverse=True)
    leaderboard_title = f"🏆 Weekly Leaderboard: {unit_name_display} 🏆" 
    return format_leaderboard(leaderboard_title, sorted_scores, start_date, end_date, unit_name_display)


# --- Discord Bot Setup ---
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# --- Timezone Setup for Scheduled Tasks ---
EST = pytz.timezone('America/New_York')

# --- Scheduled Leaderboard Posting Task (Fridays & Sundays at 2pm EST) ---
scheduled_leaderboard_time_est = dt_time(14, 0, 0, tzinfo=EST)

@tasks.loop(time=scheduled_leaderboard_time_est)
async def post_leaderboards_on_schedule():
    await bot.wait_until_ready()
    today_est = datetime.now(EST).weekday()
    if today_est in (4, 6):
        channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
        if channel:
            logger.info(f"Posting all leaderboards to channel ID: {LEADERBOARD_CHANNEL_ID} (today is {'Friday' if today_est==4 else 'Sunday'})")
            start_date, end_date = get_current_week_dates()
            activity_keys = list(ACTIVITY_MAP.keys())
            for activity_key in activity_keys:
                try:
                    embed = await fetch_and_format_activity_leaderboard(activity_key, WORKSHEET, start_date, end_date)
                    if embed is not None:
                        await channel.send(embed=embed)
                        logger.info(f"Posted {activity_key.capitalize()} leaderboard.")
                    else:
                        logger.info(f"No nonzero data for {activity_key.capitalize()} leaderboard; nothing posted.")
                except Exception as e:
                    logger.error(f"Error posting {activity_key.capitalize()} leaderboard: {e}", exc_info=True)
                    await channel.send(f"Sorry, there was an error generating the {activity_key.capitalize()} leaderboard.")
        else:
            logger.error(f"Could not find channel with ID {LEADERBOARD_CHANNEL_ID} for scheduled leaderboard posting.")
    else:
        logger.info(f"Today is not Friday or Sunday in EST. No leaderboard posting. (Current weekday: {today_est})")

# --- Sunday Reminder Task ---
scheduled_time_est = dt_time(10, 30, 0, tzinfo=EST)

dbab_emoji_id = "<:DBAB:1369689466708557896>"

@tasks.loop(time=scheduled_time_est)
async def send_sunday_reminder():
    logger.info(f"Sunday reminder task started. Scheduled for {scheduled_time_est.strftime('%H:%M:%S %Z')}.")
    await bot.wait_until_ready()
    
    if datetime.now(EST).weekday() == 6: 
        channel = bot.get_channel(REMINDER_CHANNEL_ID)
        if channel:
            message = (
                "@JUST WIN CREW **REMINDER:** As the week wraps up make sure to *submit your sales and activity numbers*. John will go over them on "
                f"the team call on Monday. (If you don't submit numbers he might call you out 😂 {dbab_emoji_id})\n\n{REMINDER_LINK}"
            )
            try:
                await channel.send(message)
                logger.info(f"Successfully sent Sunday reminder to channel ID: {REMINDER_CHANNEL_ID}")
            except discord.errors.Forbidden:
                logger.error(f"Bot lacks permission to send Sunday reminder to channel ID: {REMINDER_CHANNEL_ID}")
            except Exception as e:
                logger.error(f"Failed to send Sunday reminder: {e}", exc_info=True)
        else:
            logger.error(f"Could not find channel with ID {REMINDER_CHANNEL_ID} for Sunday reminder.")
    else:
        logger.info(f"Sunday reminder: Today is not Sunday in EST. Current EST weekday: {datetime.now(EST).weekday()}")


@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user.name} (ID: {bot.user.id})')
    print('------')
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} application commands.")
        print(f"Synced {len(synced)} slash commands.")
    except Exception as e:
        logger.error(f"Failed to sync application commands: {e}")
        print(f"Failed to sync slash commands: {e}")

    print('Bot is ready and connected to Discord.')
    print('Use the command /leaderboard to get started.')
    print('------')
    if not post_leaderboards_on_schedule.is_running():
        post_leaderboards_on_schedule.start()
        logger.info(f"Started scheduled leaderboard posting task for Fridays and Sundays at {scheduled_leaderboard_time_est.strftime('%H:%M:%S %Z')}.")
    
    if not send_sunday_reminder.is_running():
        send_sunday_reminder.start()
        logger.info(f"Started Sunday reminder task, scheduled for {scheduled_time_est.strftime('%H:%M:%S %Z')}.")


# --- Define the Slash Command ---
@bot.tree.command(name="leaderboard", description="Shows the weekly leaderboard for a selected activity.")
@app_commands.describe(activity="Select the activity for the leaderboard (e.g., Dials).")
@app_commands.choices(activity=activity_choices)
async def leaderboard_slash(interaction: Interaction, activity: app_commands.Choice[str]):
    chosen_activity_value = activity.value
    await interaction.response.defer(thinking=True, ephemeral=False)

    if chosen_activity_value not in ACTIVITY_MAP:
        valid_activities = ", ".join(c.name for c in activity_choices)
        await interaction.followup.send(
            f"Invalid activity type '{chosen_activity_value}'. Please choose from: {valid_activities}.",
            ephemeral=True
        )
        logger.warning(f"Received invalid activity '{chosen_activity_value}' from user: {interaction.user.id}.")
        return
    
    start_date_val, end_date_val = get_current_week_dates()
    embed = await fetch_and_format_activity_leaderboard(chosen_activity_value, WORKSHEET, start_date_val, end_date_val)
    
    if embed is not None:
        await interaction.followup.send(embed=embed)
        logger.info(f"Successfully generated and sent leaderboard for '{ACTIVITY_MAP[chosen_activity_value]}' (displayed as '{chosen_activity_value.capitalize()}') via slash command for user {interaction.user}.")
    else:
        await interaction.followup.send(
            f"No leaderboard data to display for {chosen_activity_value.capitalize()} this week.",
            ephemeral=True
        )
        logger.info(f"No nonzero leaderboard data for '{chosen_activity_value}' requested by user {interaction.user}.")

# --- Run the Bot ---
if __name__ == "__main__":
    if DISCORD_BOT_TOKEN:
        bot.run(DISCORD_BOT_TOKEN)
    else:
        logger.error("Discord bot token is not configured. Set DISCORD_BOT_TOKEN in the .env file.")
