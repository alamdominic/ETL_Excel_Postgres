import logging
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from config.email_credentials import emailCredentials


def send_email_report(subject, body, recipient, attachment_path):
    """Envía un correo electrónico de reporte, opcionalmente incluyendo un archivo adjunto.

    Utiliza credenciales obtenidas de forma segura (presumiblemente de variables de
    entorno a través de `emailCredentials()`) y se conecta al servidor SMTP de Gmail
    (puerto 587 con TLS) para enviar el mensaje.

    Registra un mensaje de éxito o un error de envío en el log.

    Args:
        subject (str): El asunto del correo electrónico.
        body (str): El contenido principal del cuerpo del correo (texto plano).
        recipient (str): La dirección de correo electrónico del destinatario.
        attachment_path (str): La ruta completa del archivo a adjuntar (ej. log o PDF).
            El adjunto es omitido si la ruta no existe o es None.

    Returns:
        None: La función no devuelve un valor, solo maneja el proceso de envío.
    """

    # Obtener credenciales desde variables de entorno
    sender_email, sender_password = emailCredentials()

    # Crear correo
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = recipient
    message["Subject"] = subject

    # Cuerpo del mensaje
    message.attach(MIMEText(body, "plain"))

    # Adjuntar archivo (log o pdf)
    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={os.path.basename(attachment_path)}",
        )
        message.attach(part)

    # Enviar correo
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(message)
            logging.info(f"Correo enviado a {recipient} con asunto: {subject}")
    except Exception as e:
        logging.error(f"Error al enviar el correo: {e}")
