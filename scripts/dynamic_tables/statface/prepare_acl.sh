#!/bin/sh

yt create group --opt /attributes/name=admins
yt set //@acl/end '{action=allow;permissions=[write;administer];subjects=[admins]}'
yt set //sys/accounts/sys/@acl/0/subjects/end admins
yt set //sys/tokens/0a8f523f14c3318bd3df7bd79af527fb cron
yt create user --opt /attributes/name=ignat
yt add_member ignat admins
yt create user --opt /attributes/name=cron
yt add_member cron admins
