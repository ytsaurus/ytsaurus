chown -R $USER $HOME
chown -R $USER /cache
chown -R $USER /reports

export YT_TOKEN=non_existent_token
export HOME=/app
su --shell /bin/sh --preserve-environment --command 'sbt -Duser.home=/app --sbt-dir /cache/sbt --sbt-boot /cache/sbt/boot --ivy /cache/ivy test' $USER
code=$?
su --shell /bin/sh --preserve-environment --command 'find -type d -name test-reports -exec cp -r --parents {} /reports \;' $USER

return $code
