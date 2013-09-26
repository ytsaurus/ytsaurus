#!/bin/bash -eux

for package in yandex-yt-python yandex-yt-python-tools; do
    rm -rf debian setup.py
    
    ln -s $package/debian debian
    ln -s $package/setup.py setup.py

	python setup.py clean
	sudo make -f debian/rules clean

    YT="yt/wrapper/yt"
    VERSION=$(dpkg-parsechangelog | grep Version | awk '{print $2}')
    if [ "$package" = "yandex-yt-python" ]; then
        echo "VERSION='$VERSION'" > yt/wrapper/version.py
    fi
    
    # Build and upload package
    make deb_without_test
    # dupload "../yandex-yt-python_${VERSION}_amd64.changes"

    if [ "$package" = "yandex-yt-python" ]; then
        # Upload egg
        export YT_PROXY=kant.yt.yandex.net
        DEST="//home/files"

        make egg
        EGG_VERSION=$(echo $VERSION | tr '-' '_')
        eggname="Yt-${EGG_VERSION}_-py2.7.egg"
        cat dist/$eggname | $YT upload "$DEST/Yt-${VERSION}-py2.7.egg"
        cat dist/$eggname | $YT upload "$DEST/Yt-py2.7.egg"
        
        # Upload self-contained binaries
        mv yt/wrapper/pickling.py pickling.py
        cp standard_pickling.py yt/wrapper/pickling.py
        
        for name in yt mapreduce-yt; do
            rm -rf build dist
            pyinstaller/pyinstaller.py --noconfirm --onefile yt/wrapper/$name
            pyinstaller/pyinstaller.py --noconfirm "${name}.spec"
            cat dist/$name | $YT upload "$DEST/${name}_${VERSION}"
            cat dist/$name | $YT upload "$DEST/$name"
        done
        
        mv pickling.py yt/wrapper/pickling.py
    fi

    rm -rf debian setup.py
done

