#!/usr/bin/python

from sys import exit
import yt.wrapper as yt
import logging
from email.mime.text import MIMEText
from subprocess import Popen, PIPE
import time

from argparse import ArgumentParser

logger = logging.getLogger("notificator")
formatter = logging.Formatter("%(asctime)s  %(levelname).1s  %(module)s:%(lineno)d  %(message)s")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

yt.config["proxy"]["url"] = "locke"

def send_notification(cluster, notification_id, notification, recipients, major_recipients, dry_run):
    if recipients is None:
        recipients = []
    if major_recipients is None:
        major_recipients = []
    final_recipients = recipients + ([] if notification.get("severity", "(N/A)") != "major" else major_recipients)
    if len(final_recipients) == 0:
        logger.warning("Nobody is going to receive notification %s", notification_id)
        return

    logger.debug("Notification %s content:\n%s", notification_id, notification)
   
    description = notification.get("description", "No description")
    if not ("<" in description and ">" in description):
        # Seems like the author of description forgot about html-format, let's 
        # fix newlines for him.
        description = description.replace("\n", "<br />") 
    mail_subject = "**{0}**: {1}".format(cluster, notification.get("title", "(no subject)"))
    mail_body = """<html>
    <head></head>
    <body>
        <p><b>Cluster</b>: {0}</p>
        <p>{1}</p>
        <p><b>Severity</b>: {2}</p>
        <p><b>Estimated start time</b>: {3}</p>
        <p><b>Estimated finish time</b>: {4}</p>
        <p><b>Created by</b>: {5}</p>
    </body>
</html>
""".format(
        cluster,
        description,
        notification.get("severity", "(N/A)"),
        time.ctime(notification["estimated_start_time"]) if "estimated_start_time" in notification else "(N/A)",
        time.ctime(notification["estimated_finish_time"]) if "estimated_finish_time" in notification else "(N/A)",
        notification.get("author", "(N/A)"))
    msg = MIMEText(None, "html", "utf-8")
    msg.replace_header('content-transfer-encoding', 'quoted-printable')
    msg.set_payload(mail_body)
    msg["Subject"] = mail_subject
    msg["From"] = notification.get("author", "devnull") + "@yandex-team.ru"
    msg["To"] = ", ".join(map(lambda name: name + "@yandex-team.ru", final_recipients))
    logger.debug("Sending the following mail: %s", msg.as_string())
    if not dry_run:
        p = Popen(["/usr/sbin/sendmail", "-t", "-oi"], stdin=PIPE)
        p.communicate(msg.as_string())

def process_email(config, dry_run):
    for cluster, cluster_config in config.items():
        logger.info("Processing cluster %s", cluster)
        notifications = yt.get("//sys/notifications/local/" + cluster)
        for notification_id, notification in notifications.items(): 
            if notification.get("sent_via_mail", False) or (not notification.get("published", False)):
                continue
            logger.info("Sending notification %s", notification_id)
            send_notification(cluster, 
                              notification_id, 
                              notification, 
                              cluster_config.get("send_all", []), 
                              cluster_config.get("send_major", []),
                              dry_run)
            if not dry_run:
                yt.set("//sys/notifications/local/{0}/{1}/sent_via_mail".format(cluster, notification_id), True)

def main():
    parser = ArgumentParser()
    parser.add_argument("-d", "--dry-run", action="store_true")
    args = parser.parse_args()
    
    if args.dry_run:
        logger.setLevel(logging.DEBUG)

    logger.info("Reading configuration...")
    try:
        config = yt.get("//sys/notifications/config")
    except Exception:
        logger.exception("Error while reading configuration. Didn't you forget to specify locke as YT_PROXY?")
        exit(1)

    process_email(config, args.dry_run)
                
if __name__ == "__main__":
    main()
