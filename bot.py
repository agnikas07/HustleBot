import discord
from discord.ext import commands
from discord import app_commands
from discord import Interaction # Keep this import
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

# --- Activity Mapping (for non-Sales activities) ---
ACTIVITY_MAP = {
    "dials": DIALS_COLUMN_NAME,
    "doorknocks": DOORKNOCKS_COLUMN_NAME,
    "appointments": APPOINTMENTS_COLUMN_NAME,
    "presentations": PRESENTATIONS_COLUMN_NAME,
}

# --- Create choices for the slash command argument ---
activity_choices = [
    app_commands.Choice(name=key.capitalize(), value=key)
    for key in sorted(ACTIVITY_MAP.keys())
]
activity_choices.append(app_commands.Choice(name="Sales", value="sales"))
activity_choices = sorted(activity_choices, key=lambda c: c.name)


# --- Helper Functions ---
def get_current_week_dates():
    """Calculates the start (Monday) and end (Sunday) dates of the current week."""
    today = date.today()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    end_of_week = start_of_week + timedelta(days=6)          # Sunday
    return start_of_week, end_of_week

def format_leaderboard(title, sorted_scores, start_date, end_date, unit_name_display, top_n=9):
    """Formats the leaderboard data into a Discord Embed."""
    if not sorted_scores:
        embed = discord.Embed(title=title, description="No data found for this week.", color=discord.Color.orange())
        return embed

    embed = discord.Embed(title=title, color=discord.Color.gold())
    description = f"{unit_name_display} from {start_date.strftime('%b %d')} - {end_date.strftime('%b %d')}\n\n"

    rank = 1
    for i, (name, score) in enumerate(sorted_scores[:top_n]):
        emoji = ""
        if rank == 1: emoji = "🥇 "
        elif rank == 2: emoji = "🥈 "
        elif rank == 3: emoji = "🥉 "
        else: emoji = f"{rank}️⃣. "

        embed.add_field(name=f"{emoji}{name}", value=f"{unit_name_display} completed: **{score}**", inline=False)
        rank += 1

    if not embed.fields: # Check if any fields were added (i.e., if sorted_scores was not empty)
         # This part might be redundant if the initial `if not sorted_scores:` catches it.
         # However, if top_n is 0 or sorted_scores becomes empty after slicing, this could be useful.
         description="No entries recorded for the top positions this week."


    embed.description = description
    embed.set_footer(text=f"Leaderboard generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return embed

# --- Discord Bot Setup ---
intents = discord.Intents.default()
# intents.message_content = True # Add this if your !leaderboard prefix command needs it.
                                # It's not strictly needed for slash commands alone.

# The prefix is important if "!leaderboard" is an existing prefix command.
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    """Event handler for when the bot logs in and is ready."""
    logger.info(f'Logged in as {bot.user.name} (ID: {bot.user.id})')
    print('------')
    try:
        synced = await bot.tree.sync() # Sync global commands
        logger.info(f"Synced {len(synced)} application commands.")
        print(f"Synced {len(synced)} slash commands.")
    except Exception as e:
        logger.error(f"Failed to sync application commands: {e}")
        print(f"Failed to sync slash commands: {e}")

    print(f'Bot is ready and connected to Discord.')
    print(f'Use the command /leaderboard to get started.')
    print('------')


# --- Define the Slash Command ---
# Updated descriptions to reflect the "Sales" option
@bot.tree.command(name="leaderboard", description="Shows weekly leaderboard or triggers Sales leaderboard.")
@app_commands.describe(activity="Select activity (e.g., Dials) or 'Sales' to trigger its leaderboard.")
@app_commands.choices(activity=activity_choices) # Use the updated choices
async def leaderboard_slash(interaction: Interaction, activity: str):
    """
    Slash command to fetch and display the weekly leaderboard for a specific activity,
    or trigger the '!leaderboard' command if 'Sales' is chosen.
    The 'activity' parameter value will be lowercase (e.g., "dials", "sales").
    """
    chosen_activity_value = activity.lower() # Ensure comparison is case-insensitive

    if chosen_activity_value == "sales":
        # Special handling for "Sales"
        # Defer ephemerally as we are just sending a message, not doing long processing here.
        await interaction.response.defer(ephemeral=True, thinking=False)
        try:
            target_channel = interaction.channel
            if target_channel is None: # Should generally not happen in guild context
                logger.error(f"Interaction channel is None for user {interaction.user}. Cannot send '!leaderboard'.")
                await interaction.followup.send("Could not determine the channel to send the command to.", ephemeral=True)
                return

            await target_channel.send("!leaderboard") # Post the prefix command
            logger.info(f"User {interaction.user} (ID: {interaction.user.id}) triggered '!leaderboard' (for Sales) in channel {target_channel.name} (ID: {target_channel.id}) via slash command.")
            await interaction.followup.send(
                f"The `!leaderboard` command for Sales has been posted in {target_channel.mention}.",
                ephemeral=True
            )
        except discord.errors.Forbidden:
            logger.warning(f"Bot lacks permission to send messages in channel {interaction.channel.id} for '!leaderboard' (Sales) trigger.")
            await interaction.followup.send("I don't have permission to send messages in this channel.", ephemeral=True)
        except Exception as e:
            logger.error(f"Error triggering '!leaderboard' for Sales via slash command: {e}", exc_info=True)
            await interaction.followup.send("An error occurred while trying to trigger the Sales leaderboard.", ephemeral=True)
        return # IMPORTANT: Stop further processing for "sales"

    # --- Original logic for other activities (Dials, Doorknocks, etc.) ---
    # Defer for potentially long-running Google Sheets operation
    await interaction.response.defer(thinking=True, ephemeral=False)

    # This check is now for activities OTHER THAN "sales"
    # chosen_activity_value is already lowercase here.
    if chosen_activity_value not in ACTIVITY_MAP:
        # This case should ideally not be hit if choices are correct and "sales" is handled.
        other_valid_activities = ", ".join(c.name for c in activity_choices if c.value != "sales")
        await interaction.followup.send(
            f"Invalid activity type '{activity}'. Please choose from: {other_valid_activities} or 'Sales'.",
            ephemeral=True
        )
        logger.warning(f"Received invalid activity '{activity}' (not 'sales' and not in ACTIVITY_MAP) from user: {interaction.user.id}.")
        return

    target_column_name = ACTIVITY_MAP[chosen_activity_value] # Use the lowercase value
    unit_name_display = chosen_activity_value.capitalize() # For display in embed (e.g., "Dials")
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
    # Simplified log flags, can be expanded if detailed per-row suppression is critical
    first_warning_flags = {'missing_column': False, 'date_parse': False, 'value_conversion': False}


    for record_num, record in enumerate(records, 1): # enumerate for better logging
        try:
            # Check for presence of essential columns
            if not all(col in record for col in [DATE_COLUMN_NAME, NAME_COLUMN_NAME, target_column_name]):
                if not first_warning_flags['missing_column']:
                    logger.warning(f"Row {record_num} missing one or more expected columns ({DATE_COLUMN_NAME}, {NAME_COLUMN_NAME}, or {target_column_name}). Further similar warnings for missing columns will be suppressed. Row data: {record}")
                    first_warning_flags['missing_column'] = True
                skipped_rows += 1
                continue

            date_str = record.get(DATE_COLUMN_NAME, "")
            name = record.get(NAME_COLUMN_NAME, "").strip()
            activity_value_raw = record.get(target_column_name, "")

            if not date_str or not name: # Skip if date or name is empty after stripping
                skipped_rows += 1
                continue

            try:
                record_date = datetime.strptime(str(date_str).strip(), SHEET_DATE_FORMAT).date()
            except ValueError:
                if not first_warning_flags['date_parse']:
                    logger.warning(f"Could not parse date '{date_str}' in row {record_num} using format '{SHEET_DATE_FORMAT}'. Check .env and sheet data. Further date parsing warnings suppressed. Row: {record}")
                    first_warning_flags['date_parse'] = True
                skipped_rows += 1
                continue

            if not (start_date <= record_date <= end_date):
                continue # Skip if not in the current week

            try:
                # Ensure empty strings or non-numeric strings become 0
                activity_value_str = str(activity_value_raw).strip()
                activity_value = int(activity_value_str) if activity_value_str else 0
            except (ValueError, TypeError):
                if not first_warning_flags['value_conversion']:
                    logger.warning(f"Could not convert activity value '{activity_value_raw}' in column '{target_column_name}' (row {record_num}) to integer. Treating as 0. Further value conversion warnings suppressed. Record: {record}")
                    first_warning_flags['value_conversion'] = True
                activity_value = 0

            user_scores[name] += activity_value
            processed_rows += 1

        except Exception as e: # Catch-all for unexpected errors during row processing
            logger.error(f"Error processing row {record_num}: {record}. Error: {e}", exc_info=True)
            skipped_rows += 1
            continue

    logger.info(f"Data processing complete. Processed rows for week: {processed_rows}. Skipped rows (missing/invalid data or errors): {skipped_rows}. Unique users found: {len(user_scores)}")

    sorted_scores = sorted(user_scores.items(), key=lambda item: item[1], reverse=True)

    leaderboard_title = f"🏆 Weekly Leaderboard: {unit_name_display} 🏆" # Use capitalized display name
    embed = format_leaderboard(leaderboard_title, sorted_scores, start_date, end_date, unit_name_display)

    await interaction.followup.send(embed=embed)
    logger.info(f"Successfully generated and sent leaderboard for '{target_column_name}' (displayed as '{unit_name_display}') via slash command for user {interaction.user}.")

# --- Run the Bot ---
if __name__ == "__main__":
    if DISCORD_BOT_TOKEN:
        bot.run(DISCORD_BOT_TOKEN)
    else:
        logger.error("Discord bot token is not configured. Set DISCORD_BOT_TOKEN in the .env file.")

