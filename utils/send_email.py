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

    Soporta múltiples destinatarios mediante string separado por comas o lista.

    Args:
        subject (str): El asunto del correo electrónico.
        body (str): El contenido principal del cuerpo del correo (texto plano).
        recipient (str or list): Destinatario(s). Puede ser:
            - String con un email: "user@example.com"
            - String con múltiples emails: "user1@example.com,user2@example.com"
            - Lista de emails: ["user1@example.com", "user2@example.com"]
        attachment_path (str): La ruta completa del archivo a adjuntar (ej. log o PDF).
            El adjunto es omitido si la ruta no existe o es None.

    Returns:
        None: La función no devuelve un valor, solo maneja el proceso de envío.
    """

    # Obtener credenciales desde variables de entorno
    sender_email, sender_password = emailCredentials()

    # Procesar destinatarios (soportar múltiples formatos)
    if isinstance(recipient, list):
        recipients_list = recipient
        recipients_str = ", ".join(recipient)
    elif isinstance(recipient, str):
        if "," in recipient:
            recipients_list = [email.strip() for email in recipient.split(",")]
        else:
            recipients_list = [recipient.strip()]
        recipients_str = recipient
    else:
        raise ValueError("El parámetro 'recipient' debe ser un string o una lista")

    # Crear correo
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = recipients_str
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
            # sendmail requiere lista de destinatarios
            server.sendmail(sender_email, recipients_list, message.as_string())
            logging.info(
                f"Correo enviado a {len(recipients_list)} destinatario(s): {recipients_str} con asunto: {subject}"
            )
    except Exception as e:
        logging.error(f"Error al enviar el correo: {e}")
