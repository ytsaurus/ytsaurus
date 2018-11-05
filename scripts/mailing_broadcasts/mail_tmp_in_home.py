#!/usr/bin/env python
# coding=utf-8

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import smtplib

import sys
import json
import re
import yt.wrapper as yt



MAIL_SERVER = "outbound-relay.yandex.net"

mail_body_template = u"""
<html>
    <head></head>
    <body>
    Привет!
    <br />
    <br />
    Это повторное письмо с просьбой не использовать аккаунт tmp в домашках (//home/..).
    12 ноября узлы в домашках, которые остались в аккаунте tmp, будут удалены или перенесены в ваш аккаунт. 
    При этом возможно переполнение квоты вашего аккаунта! Поэтому мы рекомендуем разобраться с этими узлами самостоятельно. 
    Ниже перечислены пути из аккаунта tmp в вашей домашней директории.
    <br />
    <br />
    <b>Домашний каталог:</b> {home}
    <br />
    <b>Аккаунт:</b> {account}
    <br />
    <b>Количество нод:</b> {count}
    <br />
    <b>Объём дискового пространства:</b> {disk_space} GB
    <br />
    <b>Пути к нодам в приложении к письму</b>
    <br />
    <br />
    Если вы по каким-то причинам хотите продолжать использовать аккаунт tmp в вашей домашке, то сообщите нам об этом с указанием причины в ответ на это письмо.
    </body>
</html>
"""


def make_stat(output_path, cluster):

    def get_disk(path):
        try:
            attrs = yt.get(path + '/@')
        except:
            return 0
        if attrs["type"] != "map_node":
            return attrs["resource_usage"]["disk_space"]
        return 0

    yt.config["proxy"]["url"] = cluster
    rx = re.compile("//home/[^/]+")
    result = {}
    folders = yt.search("//home",
                        object_filter=lambda obj: obj.attributes.get("account") == "tmp",
                        attributes=["account"])
    for folder in folders:
        try:
            if not yt.exists(folder):
                print "skip", folder
                continue

            prefix = rx.match(folder).group()

            if prefix in result:
                result[prefix]["paths"].append(folder)
                result[prefix]["disk_space"] += get_disk(folder)
                continue

            account = yt.get(prefix + "/@account")
            responsibles = yt.get("//sys/accounts/" + account + "/@responsibles") if yt.exists("//sys/accounts/" + account + "/@responsibles") else []
            result[prefix] = {
                "account": account,
                "paths": [folder],
                "disk_space": get_disk(folder),
                "responsibles": responsibles
            }
        except:
            print "bug on " + folder
            raise
    json.dump(result, open(output_path, 'w'), indent=True)


def send(stats_path):
    s = smtplib.SMTP(MAIL_SERVER)
    stats = json.load(open(stats_path))

    for key in stats:
        stat = stats[key]
        if len(stat["responsibles"]) == 0:
            print "Skipping" + key
            continue

        msg = MIMEMultipart()

        text = MIMEText(None, "html", "utf-8")
        text.replace_header("content-transfer-encoding", "quoted-printable")
        text.set_payload(
            mail_body_template.format(
                home=key,
                account=stat["account"],
                disk_space=stat["disk_space"] / (1024**3),
                count=len(stat["paths"]),
            ).encode("utf-8"))
        msg.attach(text)

        attachment = MIMEApplication("\n".join(stat["paths"]), Name="paths.txt")
        attachment['Content-Disposition'] = 'attachment; filename="paths.txt"'
        msg.attach(attachment)

        # stat["responsibles"] = ["andozer"]

        msg["Subject"] = u"[Important] Использование аккаунта tmp в домашках"
        msg["From"] = "renadeen@yandex-team.ru"
        mails = [r + "@yandex-team.ru" for r in stat["responsibles"]]
        msg["To"] = ", ".join(mails)
        msg["Reply-To"] = "yt-migration@yandex-team.ru"
        s.sendmail("renadeen@yandex-team.ru", mails + ["renadeen@yandex-team.ru"], msg.as_string())
        # break

    s.quit()


if __name__ == "__main__":
    if sys.argv[1] == "make_stat":
        make_stat(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == "send_mail":
        send(sys.argv[2])
    else:
        raise Exception("Wrong arguments")
