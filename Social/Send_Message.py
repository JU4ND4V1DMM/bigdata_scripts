import os
import time
from datetime import datetime
from random import randint
import csv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC  # Add this import
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, regexp_replace
from selenium.common import exceptions as selexceptions
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager  # Add this import

# Create the Spark session
spark = SparkSession.builder \
    .appName("Excel to DataFrame") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
    .getOrCreate()
    
spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs", "false")

sqlContext = SQLContext(spark)

# Function to log in to WhatsApp Web

def login_whatsapp(driver: WebDriver):
    try:
        print("Starting login process to WhatsApp Web")
        driver.get('https://web.whatsapp.com/')
        print("Waiting for login...")
        # Wait until the new chat button is visible (indicating the session is logged in)
        new_chat_btn = WebDriverWait(driver, 90).until(EC.element_to_be_clickable((
            By.XPATH,
            '//*[@id="app"]/div/div[3]/div/div[3]/header/header/div/span/div/div[1]/button'
        )))
        time.sleep(2)  # Wait a few seconds after loading WhatsApp Web

    except selexceptions.TimeoutException:
        print("Timeout waiting to log in")
        driver.quit()

# Function to send messages
def send_message(driver: WebDriver, phone_number: str, message: str, sleep: int = 2):
    try:
        # Find and click the new chat button
        new_chat_btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((
            By.XPATH,
            '//*[@id="app"]/div/div[3]/div/div[3]/header/header/div/span/div/div[1]/button'
        )))
        new_chat_btn.click()
        time.sleep(randint(1, 5))

        # Find the search box and enter the phone number
        text_box = WebDriverWait(driver, 3).until(EC.presence_of_element_located((
            By.XPATH,
            '//*[@id="app"]/div/div[3]/div/div[2]/div[1]/span/div/span/div/div[1]/div[2]/div/div/div[1]/p'
        )))
        text_box.clear()
        text_box.send_keys(phone_number)
        print(f"Phone number entered: {phone_number}")
        time.sleep(1)

        # Wait and check if there is a chat with that number
        chat_exist = WebDriverWait(driver, 3).until(EC.presence_of_element_located((
            By.CLASS_NAME,
            '_ak8l'
        )))

        if chat_exist:
            chat_exist.click()  # Open the chat
            print(f"Chat opened for: {phone_number}")
            time.sleep(3)

            # Locate the message box and send the message
            try:
                message_box = WebDriverWait(driver, 10).until(EC.presence_of_element_located((
                    By.XPATH,
                    '//*[@id="main"]//footer//div[@contenteditable="true"]'
                )))
                message_box.click()
                message_box.clear()
                message_box.send_keys(message)
                time.sleep(randint(2, 3))
                message_box.send_keys(Keys.ENTER)
                print(f"Message sent to: {phone_number}")
                return True
            except selexceptions.StaleElementReferenceException:
                print(f"Stale element reference while sending message to: {phone_number}")
                return True
            except TimeoutException:
                print(f"Timeout waiting for message box for: {phone_number}")
                return False
        else:
            print(f"Chat not found for: {phone_number}")
            return False
    except Exception as e:
        search_box = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, '//*[@id="app"]/div/div[3]/div/div[2]/div[1]/span/div/span/div/div[1]/div[2]/div/div/div[1]/p')))
        search_box.send_keys(Keys.ESCAPE)
        print(f"Error sending message to {phone_number}: {e}")
        return False

def process_and_send(Path, Outpath, Root_Number, Phone, Operator, start_time_process, end_time_process):
    # Read the CSV file
    df = spark.read \
        .format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(Path)

    # Check if DataFrame is empty
    if df.count() == 0:
        print("DataFrame is empty. No data to process.")
        return

    # Initialize WebDriver
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(service=ChromeService(
        ChromeDriverManager().install()), options=chrome_options)

    # Log in to WhatsApp Web
    login_whatsapp(driver)

    # Prepare the output list
    sent_count = 0
    not_sent_count = 0

    # Write headers to the output file if it doesn't exist
    if not os.path.isfile(Outpath):
        with open(Outpath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Edad_Mora", "CRM", "Cuenta", "Dato_Contacto", "Mensaje",
                             "Estado", "MIN Remitente", "Operador", "Dispositivo", "Hora_Envio", "Hora"])

    # Iterate over the rows of the DataFrame
    for row in df.collect():
        now = datetime.now()

        if start_time_process <= (now.hour, now.minute) <= end_time_process:
            cuenta = row["Cuenta"]
            nombre = row["SMS"]
            entidad = row["CRM"]
            mora = row["Edad_Mora"]
            saldo_total = row["SALDO_TOTAL"]
            valor_pago = row["VALOR_PAGO"]
            phone_number = row["Dato_Contacto"]
            estado = "Not sent"

            mensaje = message_to_send(cuenta, nombre, saldo_total, valor_pago, entidad, mora)
            
            print(f'Processing number: {phone_number}')

            # Try to send the message
            success = send_message(driver, phone_number, mensaje)

            if success:
                estado = "Sent"
                sent_count += 1
            else:
                not_sent_count += 1

            # Get the current time
            hora_envio = now.strftime("%Y-%m-%d %H:%M:%S")
            hora = now.strftime("%H")

            # Write the result to the CSV file
            with open(Outpath, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([row["Edad_Mora"], row["CRM"], cuenta, phone_number,
                                 "Mensaje Personalizado", estado, Root_Number, Operator, Phone, hora_envio, hora])

            time.sleep(randint(1, 3))  # Reduce wait time between messages
        else:
            print("Message not sent due to time restrictions.")

    # Close the browser
    driver.quit()
    print("Process completed.")

def message_to_send(cuenta, nombre, saldo_total, valor_pago, entidad, mora):
    
    # Predefine messages to avoid recreating them in each iteration
    mensaje_QNT = (
        ", te ofrecemos una oportunidad para comenzar una nueva etapa en tu historial crediticio.\n"
        "Puedes elegir la forma que mejor se adapte a tus necesidades y te beneficiarás en:\n"
        "* _Reduciendo tu deuda pagando menos_\n"
        "* _Generando reportes positivos en centrales de riesgo_ \n"
        "* _Mejorando tu historial crediticio mes a mes_ \n"
        "* _Incrementando tu *puntaje*_ \n"
        "Con QNT, desde el primer pago tu compromiso estará al día en centrales de riesgo como Datacrédito o TransUnion. *Comunícate con nosotros al 313 273 6944 y recibe atención personalizada*"
    )
    
    mensaje_QNT_2 = (
        f"*¡ATENCIÓN! Oportunidad ÚNICA para ponerse al día con tu deuda {nombre}*\n"
        f"¡Felicitaciones! (‾◡◝) Has sido seleccionado para recibir un descuento exclusivo con *{entidad}*\n"
        f"* *Deuda total actual:* _{saldo_total}_\n"
        f"* *¡Paga solo HOY! desde:* _{valor_pago}_\n"
        "* Beneficios inmediatos si aprovechas *HOY*: \n"
        "* _Paz y salvo en solo 24 horas_\n"
        "* _Te ayudamos a volver al sector financiero (Rebancarización)_\n"
        "* _Diagnóstico financiero personalizado_\n"
        "* _Se actualizan tus reportes negativos en los primeros 10 días del mes después de tu pago_\n"
        "Responde este mensaje AHORA para más detalles o para negociar tu pago. *¡Te ayudamos en todo el proceso!*\n"
        "Oferta válida por tiempo limitado. No dejes pasar esta oportunidad ( ͡~ ͜ʖ ͡°)\n"
    )
    
    mensaje_QNT_3 = (
        f"¡Felicitaciones *{nombre}*! (‾◡◝)\n"
        f"Tienes una *GRAN OPORTUNIDAD* para resolver tu situación financiera. Salda tu deuda de _{saldo_total}_ con *{entidad}* y paga solo _{valor_pago}_ ¡Es el momento perfecto para recuperar tu tranquilidad!\n"
        "*Beneficios exclusivos:* Paz y salvo en 24 horas, diagnóstico financiero personalizado para alcanzar tus metas, actualización de tu historial en los primeros 10 días tras el pago. _(Campaña valida hasta el 2 de mayo)_ \n"
        "¡No dejes pasar esta oportunidad única! Haz clic aquí para hablar con un asesor y obtener más detalles: _https://wa.me/573132736944_ Recupera tu acceso al sector financiero ahora. *¡Tu futuro te lo agradecerá!* \n"
        "Oferta válida por tiempo limitado. No dejes pasar esta oportunidad ( ͡~ ͜ʖ ͡°)\n"
    )

    mensaje_GMAC = (
        f"{nombre}, le contactamos desde *GM FINANCIAL* para ofrecerle una oportunidad única de normalizar su deuda.\n"
        "Queremos brindarle la posibilidad, de pagar el saldo pendiente con un importante *descuento %*, lo que le permitirá obtener su paz y salvo, mejorar su reporte en las centrales de riesgo y, adicionalmente, evitar el iniciar acciones legales, para recuperación de dicho saldo.\n"
        "¡_Este es el momento ideal para ponerse al día y aprovechar este *beneficio exclusivo*_! (‾◡◝)\n"
        "Se mantendrá solo por tiempo limitado. No deje pasar esta oportunidad de mejorar su situación financiera de manera rápida y sencilla ( ͡~ ͜ʖ ͡°)\n"
        "Contáctenos ahora para saber más sobre esta campaña _Quedamos a la espera de su respuesta por este medio o al telefono 6017560290 Opción 2_.\n"
    )

    mensaje_QNT_2 = (
        f"¡Hola *{nombre}*! *¿Aburrido de tener un reporte negativo en centrales?* ;_; \n"
        f"¿Qué tal si saldas tu deuda pagando solo el *{saldo_total}%*? Eso significa que con solo *{valor_pago}* puedes ponerte al día \n"
        f"Promo válida hasta el 30 de abril de 2025. Aplican TyC* \n"
        f"( ͡~ ͜ʖ ͡°) Chatea con nosotros ahora y concretamos: https://wa.link/ar0dsv \n"
    )
    
    mensaje_PASH = (
        f"¡Hola *{nombre}*! *GRAN DESCUENTO con {entidad}*. Tienes un atraso, PAGA HOY {valor_pago} en tiendas {mora} y queda a paz y salvo. ( ͡~ ͜ʖ ͡°) Chatea con nosotros ahora: _3227846952_ \n"
    )
    
    mensaje_PASH_2 = (
        f"{nombre}\n"
    )
    
    mensaje_CLARO = (
        f"Cuida tu historial Crediticio Paga la factura Hogar Claro ref {valor_pago} por {saldo_total} evita reportes negativos Inf wa.link/ec0hev Si pagaste omite msj(15)\n"
    )
    
    return mensaje_CLARO
    
## User variables
user = "c.operativo"

# Call the main function
Date = "2025-06-11"  # Date for the file name
Path = f"C:/Users/{user}/Downloads/Plantilla WhatsApp - 2025.xlsx"
Outpath = f"C:/Users/{user}/Downloads/CLARO Detalle Ejecución RPA WhatsApp {Date}.csv"
Root_Number = 3202680360  # NÚMERO DESDE EL CUÁL SE ENVÍA
Operator = "Claro"  # Operador de número desde el cuál se envía
Phone = f"IMEI 86033406868{user} - Color Negro"  # Teléfono desde el cál se envía
start_time_process = (8, 0)
end_time_process = (18, 59)

# Call the function to process and send messages
process_and_send(Path, Outpath, Root_Number, Phone,
                 Operator, start_time_process, end_time_process)