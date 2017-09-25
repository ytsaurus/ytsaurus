#!/usr/bin/python

import logging
import time
from argparse import ArgumentParser
from email.mime.text import MIMEText
from subprocess import PIPE, Popen
from sys import exit

import yt.wrapper as yt

import calendar_api


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


def add_notification_to_calendar(calendar_proxy, calendar_id, cluster, notification, dry_run):
    author = (notification["author"] + "@") if "author" in notification else "(N/A)"
    description = "{0}\n\nSeverity: {1}\nAuthor: {2}".format(notification.get("description", "(No description given)"),
                                                             notification.get("severity", "(N/A)"),
                                                             author)
    name = "**{}**: {}".format(cluster, notification.get("title", ""))

    if not dry_run:
        calendar_proxy.create_event(calendar_id,
                                    name,
                                    description,
                                    time.gmtime(notification["estimated_start_time"]),
                                    time.gmtime(notification["estimated_finish_time"]))


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
        raise MalformedNotificationError("'type' must be one of 'issue', 'maintenance' or 'feature'", notification)
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


def get_notifications_path(config, cluster):
    if cluster == "global":
        return config["global_notifications_path"]
    else:
        return "{}/{}".format(config["cluster_notifications_root_path"], cluster)


def add_calendars_for_clusters(config, config_path):
    calendar_proxy = calendar_api.CalendarProxy(config["calendar_backend"], config["user_id"])
    for cluster, cluster_config in config["rules"].items():
        if "calendar_id" not in cluster_config:
            calendar_id = calendar_proxy.create_calendar("{0} maintenance".format(cluster), True)
            cluster_config["calendar_id"] = calendar_id
            yt.set("{0}/rules/{1}/calendar_id".format(config_path, cluster), calendar_id)


def email_notifications(config, dry_run):
    for cluster, cluster_config in config["rules"].items():
        notifications_path = get_notifications_path(config, cluster)
        notifications = yt.get(notifications_path)
        logger.info("Processing cluster %s", cluster)
        for notification_id, notification in notifications.items():
            if not notification.get("sent_via_mail", False) and notification.get("published", False):
                logger.info("Mailing notification %s", notification_id)
                try:
                    send_notification(cluster,
                                      notification_id,
                                      notification,
                                      cluster_config.get("send_all", []),
                                      cluster_config.get("send_major", []),
                                      dry_run)
                except MalformedNotificationError as exception:
                    logger.exception("Malformed notification: %s\n%s", exception.message, exception.notification)
                if not dry_run:
                    yt.set("{0}/{1}/sent_via_mail".format(notifications_path, notification_id), True)


def add_notifications_to_calendars(config, dry_run):
    calendar_proxy = calendar_api.CalendarProxy(config["calendar_backend"], config["user_id"])
    for cluster, cluster_config in config["rules"].items():
        notifications_path = get_notifications_path(config, cluster)
        notifications = yt.get(notifications_path)
        logger.info("Processing cluster %s", cluster)
        for notification_id, notification in notifications.items():
            if not notification.get("added_to_calendar", False) and \
                    notification.get("published", False) and \
                    notification.get("type") == "maintenance":
                logger.debug("Notification times: %s -- %s",
                             time.ctime(notification["estimated_start_time"]),
                             time.ctime(notification["estimated_finish_time"]))
                logger.info("Adding notification %s to calendar", notification_id)
                try:
                    add_notification_to_calendar(calendar_proxy,
                                                 cluster_config["calendar_id"],
                                                 cluster,
                                                 notification,
                                                 dry_run)
                except MalformedNotificationError as exception:
                    logger.exception("Malformed notification: %s\n%s", exception.message, exception.notification)
                except calendar_api.HttpResponseNotOkError as exception:
                    logger.exception("HTTP response not OK: %s", exception.message)
                if not dry_run:
                    yt.set("{0}/{1}/added_to_calendar".format(notifications_path, notification_id), True)


def main():
    parser = ArgumentParser()
    parser.add_argument("-d", "--dry-run", action="store_true")
    parser.add_argument("-c", "--config", default="//sys/notifications/config")
    args = parser.parse_args()

    if args.dry_run:
        logger.setLevel(logging.DEBUG)

    logger.info("Reading configuration...")
    try:
        config = yt.get(args.config)
    except Exception:
        logger.exception("Error while reading configuration from '%s'. Didn't you forget to specify locke as YT_PROXY?", args.config)
        exit(1)

    add_calendars_for_clusters(config, args.config)
    add_notifications_to_calendars(config, args.dry_run)
    email_notifications(config, args.dry_run)


if __name__ == "__main__":
    main()
