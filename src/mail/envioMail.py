#!/usr/bin/python
# -*- coding: utf-8 -*-

from email.mime.text import MIMEText
from smtplib import SMTP
from datetime import datetime


from_address = ''
password = ''

def setUserAndPass(user, password):
    globals()['from_address'] = user
    globals()['password'] = password

def enviarMailLog(email, texto):
    to_address = email

    message = texto
    
    message = message + "\n\n\n\nPor favor, no conteste a este mail. Si ha recibido este correo por error háganoslo saber. \nMuchas gracias."
    
    mime_message = MIMEText(message, "plain")
    mime_message["From"] = from_address
    mime_message["To"] = to_address
    mime_message["Subject"] = "BATCH FACTURAS LA VOZ DE GALICIA"
    
    smtp = SMTP("smtp.office365.com", 587)
    smtp.connect("smtp.office365.com", 587)
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()
    smtp.login(from_address, password) #fecha de nacimiento 1/01/1990
    
    #"Notificaciones"
    smtp.sendmail(from_address, to_address, mime_message.as_string())
    smtp.quit()

def envioMensaje(email, texto):
    
        to_address = email

        message = texto
    
        message = message + "\n\n\n\nPor favor, no conteste a este mail. Si ha recibido este correo por error háganoslo saber. \nMuchas gracias."
    
        mime_message = MIMEText(message, "plain")
        mime_message["From"] = from_address
        mime_message["To"] = to_address
        mime_message["Subject"] = "PROCESO FACTURAS LA VOZ DE GALICIA"
    
        smtp = SMTP("smtp.office365.com", 587)
        smtp.connect("smtp.office365.com", 587)
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()
        smtp.login(from_address, password) #fecha de nacimiento 1/01/1990
    
    #"Notificaciones"
        smtp.sendmail(from_address, to_address, mime_message.as_string())
        smtp.quit()
    