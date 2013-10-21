import smtplib, email, os
import config


def send_mail(subject, message, attach_file = None):
	"""
	Sends a mail to the recipient configured in config.py
	"""
	

	msg = email.MIMEMultipart.MIMEMultipart()	
	body = email.MIMEText.MIMEText(message)

	msg.attach(body)
	msg.add_header('From', config.smtp_from)
	msg.add_header('To', ", ".join(config.smtp_to))
	msg.add_header('Subject', subject)


	if attach_file != None:
		attachment = email.MIMEBase.MIMEBase('text', 'plain')
		attachment.set_payload(open(attach_file).read())
		attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attach_file))
		email.encoders.encode_base64(attachment)
		msg.attach(attachment)

	mailer = smtplib.SMTP(config.smtp_host, config.smtp_port)
	if config.smtp_username != None and config.smtp_password != None:
		mailer.login(config.smtp_username, config.smtp_password)
	mailer.sendmail([config.smtp_from], config.smtp_to, msg.as_string())


