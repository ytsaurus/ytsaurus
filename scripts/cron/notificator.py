#!/usr/bin/python

from sys import exit
import yt.wrapper as yt
import logging
from email.mime.text import MIMEText
from subprocess import Popen, PIPE
import time

from argparse import ArgumentParser

class MalformedNotificationError(ValueError):
    def __init__(self, message, notification):
        super(MalformedNotificationError, self).__init__(message)
        self.notification = notification


logger = logging.getLogger("notificator")
formatter = logging.Formatter("%(asctime)s  %(levelname).1s  %(module)s:%(lineno)d  %(message)s")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

yt.config["proxy"]["url"] = "locke"

mail_body_templates = {
    "issue": """<html>
        <head></head>
        <body>
            <p><b>Issue on cluster</b>: {cluster}</p>
            <p>{description}</p>
            <p><b>Severity</b>: {severity}</p>
            <p><b>Created by</b>: {author}</p>
        </body>
    </html>
    """,

    "maintenance": """<html>
        <head></head>
        <body>
            <p><b>Maintenance on cluster</b>: {cluster}</p>
            <p>{description}</p>
            <p><b>Severity</b>: {severity}</p>
            <p><b>Estimated start time</b>: {estimated_start_time}</p>
            <p><b>Estimated finish time</b>: {estimated_finish_time}</p>
            <p><b>Created by</b>: {author}</p>
        </body>
    </html>
    """,

    "feature": """<html>
        <head></head>
        <body>
            <p><b>Feature</b></p>
            <p>{description}</p>
            <p><b>Created by</b>: {author}</p>
        </body>
    </html>
    """
}


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
    is_feature = (notification.get("type") == "feature")
    mail_subject = "**{0}**: {1}".format("feature" if is_feature else cluster,
                                          notification.get("title", "(no subject)"))
    notification_type = notification.get("type")
    if notification_type is None: 
        raise MalformedNotificationError("must have 'type' field", notification)
    if notification_type not in ["issue", "maintenance", "feature"]:
        raise MalformedNotificationError("type must be one of 'issue', 'maintenance' or 'feature'", notification)
    mail_body = mail_body_templates[notification_type].format(
        cluster=cluster,
        description=description,
        severity=notification.get("severity", "(N/A)"),
        estimated_start_time=time.ctime(notification["estimated_start_time"]) if "estimated_start_time" in notification else "(N/A)",
        estimated_finish_time=time.ctime(notification["estimated_finish_time"]) if "estimated_finish_time" in notification else "(N/A)",
        author=notification.get("author", "(N/A)"))
    msg = MIMEText(None, "html", "utf-8")
    msg.replace_header('content-transfer-encoding', 'quoted-printable')
    msg.set_payload(mail_body)
    msg["Subject"] = mail_subject
    msg["From"] = notification.get("author", "devnull") + "@yandex-team.ru"
    msg["To"] = ", ".join(map(lambda name: name + "@yandex-team.ru", final_recipients))
    logger.debug("Sending the following mail: %s", msg.as_string())
    if not dry_run:
        cmd = ["/usr/sbin/sendmail", "-t", "-oi"]
        proc = Popen(cmd, stdin=PIPE)
        proc.communicate(msg.as_string())
        if proc.returncode != 0:
            raise CalledProcessError(proc.return_code, " ".join(cmd))

def process_email(config, dry_run):
    for cluster, cluster_config in config.items():
        if cluster == "global":
            logger.info("Processing global notifications")
            notifications_url = "//sys/notifications/global"
        else:
            logger.info("Processing cluster %s", cluster)
            notifications_url = "//sys/notifications/local/" + cluster
        notifications = yt.get(notifications_url)
        for notification_id, notification in notifications.items(): 
            if notification.get("sent_via_mail", False) or (not notification.get("published", False)):
                continue
            logger.info("Sending notification %s", notification_id)
            try:
                send_notification(cluster, 
                                  notification_id, 
                                  notification, 
                                  cluster_config.get("send_all", []), 
                                  cluster_config.get("send_major", []),
                                  dry_run)
            except MalformedNotificationError as exception:
                logger.error("Malformed notification: %s\n%s", exception.message, exception.notification)
            if not dry_run:
                yt.set("{0}/{1}/sent_via_mail".format(notifications_url, notification_id), True)

def main():
    parser = ArgumentParser()
    parser.add_argument("-d", "--dry-run", action="store_true")
    parser.add_argument("-c", "--config",  default="//sys/notifications/config")
    args = parser.parse_args()
    
    if args.dry_run:
        logger.setLevel(logging.DEBUG)

    logger.info("Reading configuration...")
    try:
        config = yt.get(args.config)
    except Exception:
        logger.exception("Error while reading configuration from '%s'. Didn't you forget to specify locke as YT_PROXY?", args.config)
        exit(1)

    process_email(config, args.dry_run)
                
if __name__ == "__main__":
    main()
