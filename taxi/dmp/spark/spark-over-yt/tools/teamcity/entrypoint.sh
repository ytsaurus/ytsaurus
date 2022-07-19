groupadd -o -g $HOST_DOCKER_GROUP_ID host_docker
useradd -M -d $HOME -g host_docker -G docker -o -u $UID -s /bin/sh $USER

chown -R $USER /cache
chown -R $USER /reports

export YT_TOKEN=${YT_TOKEN:-non_existent_token}

cd $PROJECT_ROOT
su --shell /bin/sh --preserve-environment --command "sbt -Duser.home=/app --sbt-dir /cache/sbt --sbt-boot /cache/sbt/boot --ivy /cache/ivy $SBT_COMMAND" $USER
code=$?
su --shell /bin/sh --preserve-environment --command 'find -type d -name test-reports -exec cp -r --parents {} /reports \;' $USER

return $code
