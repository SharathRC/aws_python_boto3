import boto3
from loguru import logger
import pprint
import json

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def verify_email(email:str):
    ses_client = boto3.client('ses')

    response = ses_client.verify_email_address(
        EmailAddress=email
    )

    logger.debug('verification email sent')
    logger.info(response)

def list_email_identities():
    ses_client = boto3.client('ses')

    response = ses_client.list_identities(
        IdentityType='EmailAddress'
    )

    logger.info(response['Identities'])

def create_email_template(
    template_name:str='CustomTemplate',
    subject:str='',
    body:str='',
    html:str='',
):
    ses_client = boto3.client('ses')

    response = ses_client.create_template(
        Template={
            'TemplateName':template_name,
            'SubjectPart':subject,
            'TextPart':body,
            'HtmlPart':html,
        }
    )

    logger.info(response)

def get_templates():
    ses_client = boto3.client('ses')

    response = ses_client.list_templates()
    logger.info(response['TemplatesMetadata'])
    pprint(response)

def send_email(
    email:str, 
    to_emails:list[str],
    template:str='custom_template',
    cc_emails:list[str]=[],
):
    ses_client = boto3.client('ses')

    resp = ses_client.send_templated_email(
        Source=email,
        Destination={
            f'ToAddresses':to_emails,
            'CcAddresses':cc_emails,
        },
        ReplyToAddresses=['youremail@com.com'],
        Template=template,
        TemplateData='{"replace":"value"}'
    )

    logger.info(resp)

def send_email_text(
    email:str, 
    to_emails:list[str],
    cc_emails:list[str]=[],
    subject:str='',
    body:str='',
):
    ses_client = boto3.client('ses')
    CHARSET='UTF-8'

    response = ses_client.send_email(
        Destination={
            "ToAddresses":to_emails
        },
        Message={
            "Body":{
                "Text":{
                    "Charset":CHARSET,
                    "Data":body
                }
            },
            "Subject":{
                "Charset":CHARSET,
                "Data":subject,
            }
        },
        Source = email
    )

    logger.info(response)

def send_html_email(
    email:str, 
    to_emails:list[str],
    cc_emails:list[str]=[],
    subject:str='',
    html_content:str='',
):
    ses_client = boto3.client('ses')
    CHARSET="UTF-8"

    response = ses_client.send_email(
        Destination={
            "ToAddresses":to_emails
        },
        Message={
            "Body":{
                "Html":{
                    "Charset":CHARSET,
                    "Data":html_content
                }
            },
            "Subject":{
                "Charset":CHARSET,
                "Data":subject
            }
        },
        Source=email
    )

    logger.info(response)

def send_email_attachment(
    email:str, 
    to_emails:list[str],
    cc_emails:list[str]=[],
    subject:str='',
    body:str='',
    attachment_file:str='',
):
    msg = MIMEMultipart()

    msg["Subject"]=subject
    msg["From"]=email
    msg["To"]=to_emails[0]

    body = MIMEText(body)
    msg.attach(body)


    with open(attachment_file, "rb") as f:
        part = MIMEApplication(f.read())
        part.add_header("Content-Disposition",
                        "attachment",
                        filename=attachment_file)

    msg.attach(part)


    ses_client = boto3.client('ses')
    response = ses_client.send_raw_email(
        Source=email,
        Destinations=to_emails,
        RawMessage={"Data":msg.as_string()}

    )

    logger.info(response)
    

# verify_email(email='sharathr.sh@gmail.com')
# list_email_identities()
# create_email_template(
#     template_name='custom_template',
#     subject='test subject',
#     body='none',
#     html='none',
# )
# get_templates()
# send_email(
#     email='sharathr.sh@gmail.com',
#     to_emails=['sharathr.sh@gmail.com'],
#     template='custom_template',
# )
# send_email_text(
#     email='sharathr.sh@gmail.com',
#     to_emails=['sharathr.sh@gmail.com'],
#     subject='new subject',
#     body='new stuff here',
# )

# html_content = """
#     <html>
#         <head></head>
#         <h1 style='text_align:center'>AWS with Python & Boto3</h1>
#         <p style='color:red'>Welcome to the course and thanks for buying the course</p>
#     </html>
# """
# send_html_email(
#     email='sharathr.sh@gmail.com',
#     to_emails=['sharathr.sh@gmail.com'],
#     subject='new html subject',
#     html_content=html_content,
# )

# send_email_attachment(
#     email='sharathr.sh@gmail.com',
#     to_emails=['sharathr.sh@gmail.com'],
#     subject='new attachment',
#     body='sample attachment here',
#     attachment_file='resources/moviedata.json'
# )