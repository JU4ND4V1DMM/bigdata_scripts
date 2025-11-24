from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib.parse
import time
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.common import exceptions as selexceptions
from selenium.webdriver.common.keys import Keys
import tkinter as tk
from tkinter import filedialog
import polars as pl
import re
import csv
from datetime import datetime


def select_file():
    root = tk.Tk()
    root.withdraw()
    file = filedialog.askopenfilename()
    return file


def select_folder():
    root = tk.Tk()
    root.withdraw()  # Hide the main window
    folder = filedialog.askdirectory(title="Select a folder")
    return folder


def create_whatsapp_url(phone_number, message):
    if not isinstance(phone_number, str) or not phone_number.isdigit():
        raise ValueError("Phone number must be a string of digits.")
    encoded_message = urllib.parse.quote(message)
    return f"https://wa.me/{phone_number}?text={encoded_message}"


def wait_for_whatsapp_login(driver: WebDriver):
    try:
        print("Opening WhatsApp Web...")
        driver.get('https://web.whatsapp.com/')
        print("Waiting for user to log in (scan the QR code)...")

        # Wait for the chats pane to appear
        WebDriverWait(driver, 90).until(
            EC.presence_of_element_located((By.ID, "pane-side"))
        )
        print("✅ Session started in WhatsApp Web.")
        time.sleep(2)
        return True

    except selexceptions.TimeoutException:
        print("❌ Login timeout.")
        driver.quit()
        exit()
        return False


def read_file(selected_file):
    # Convert to dataframe
    df = pl.read_csv(selected_file, separator=";")
    df.columns = [col.upper() for col in df.columns]
    columns = df.columns
    df.select(columns)
    columns = [col.upper() for col in df.columns]

    # Ask user for message template
    print("Available columns:", [col.upper() for col in df.columns])
    template = input("Enter the message with columns in parentheses: ")
    print(template)

    # Generate message and new CSV
    parts = re.split(r"(\([^)]+\))", template)  # Split into text and columns
    elements = []

    for part in parts:
        if part.startswith("(") and part.endswith(")"):
            col_name = part[1:-1]
            if col_name not in columns:
                print(f"⚠️ Column '{col_name}' does not exist.")
                return
            elements.append(pl.col(col_name))
        else:
            if part.strip() != "":
                elements.append(pl.lit(part))

    # Create new column with the message
    df = df.with_columns(
        pl.concat_str(elements).alias("MESSAGE")
    )

    df.write_csv("base_messages.csv")

    validation = input(
        "Please review the file base_messages.csv and type 'yes' to send the messages: ")

    if validation.upper() != "YES":
        return False
    return True, df


def send_messages():
    print('Select the file')
    file = select_file()
    print('Select the folder where the report will be saved')
    report_folder = select_folder()

    response, df = read_file(file)
    if response == False:
        return

    # Configure the browser once
    options = Options()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=Service(), options=options)

    # Open WhatsApp Web and log in once
    driver.get("https://web.whatsapp.com")
    print("🔒 Waiting for you to log in to WhatsApp Web...")
    WebDriverWait(driver, 60).until(
        EC.presence_of_element_located((By.ID, "pane-side"))
    )
    print("✅ Logged in successfully.")

    numbers = df["CELULAR"].to_list()
    messages = df["MESSAGE"].to_list()
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_for_file = datetime.now().strftime("%Y-%m-%d")
    status = None

    for number, message in zip(numbers, messages):
        url = f"https://wa.me/{number}?text={message}"
        print(f"📨 Sending to {number} -> {message}")
        driver.get(url)

        try:
            # Click on "Continue to chat"
            button = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "action-button"))
            )
            button.click()

            # Click on "use WhatsApp Web"
            web = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.LINK_TEXT, "use WhatsApp Web"))
            )
            web.click()

            # Wait for text box and send message
            message_box = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((
                    By.XPATH, '//*[@id="main"]//footer//div[@contenteditable="true"]'
                ))
            )
            time.sleep(1)
            message_box.send_keys(Keys.ENTER)
            time.sleep(2)

            status = True

        except Exception as e:
            status = False
            print(f"⚠ Error with number {number}: {e}")

        with open(f"{report_folder}/mass_report_{date_for_file}.csv", 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([number, message, current_datetime, status])

    driver.quit()


if __name__ == "__main__":
    send_messages()