#!/usr/bin/env python
# coding=utf-8

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import json
import smtplib

MAIL_SERVER = "outbound-relay.yandex.net"

mail_body_template = u"""
<html>
    <head></head>
    <body>
    Привет!
    <br />
    <br />
    В yt продолжительное время существует возможность "заимствовать" квоту из аккаунта tmp, а именно руками переносить в этот аккаунт таблицы и папки, лежащие в //home.
    При этом автоматика эти таблицы не удалит, и квота tmp расходуется, пока таблицы кто-нибудь не удалит.
    Мы хотим это изменить и в ближайшем времени планируем запускать скрипт очистки в //home и удалять все таблицы, лежащие в аккаунте tmp.
    Наша автоматика не способна быстро находить такие таблицы (чтобы их найти, нужно просканировать весь //home), 
    поэтому мы просим не создавать и не переносить в аккаунт tmp таблицы в //home, особенно не использовать эту схему в продакшен процессах. 
    Вместо этого вы можете: оставить таблицу в своей квоте, перенести в папку //tmp (аккаунт должен смениться автоматически) или удалить таблицу.
    Чтобы после включения очистки максимально снизить вероятность потери ценных данных, мы просим вас удалить или перенести эти данные в //tmp.
    Если вы не сделаете это через две недели, мы попытаемся сделать это сами.
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
    Весь список можем предоставить по запросу.
    <br />
    <br />
    Если вы по каким-то причинам хотите продолжать использовать эту схему, то сообщите нам об этом с указанием причины в ответ на это письмо.
    </body>
</html>
"""


def main():
    s = smtplib.SMTP(MAIL_SERVER)
    stats = json.load(open("/home/renadeen/purgetmp2/stats.json"))

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

        msg["Subject"] = u"[Important] Использование аккаунта tmp в домашках"
        msg["From"] = "renadeen@yandex-team.ru"
        mails = [r + "@yandex-team.ru" for r in stat["responsibles"]]
        msg["To"] = ", ".join(mails)
        msg["Reply-To"] = "yt-migration@yandex-team.ru"
        s.sendmail("renadeen@yandex-team.ru", mails + ["renadeen@yandex-team.ru"], msg.as_string())

    s.quit()


if __name__ == "__main__":
    main()
