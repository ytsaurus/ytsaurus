#!/usr/bin/make -f

build:
	echo "Build"

install:
	echo "Using $(DESTDIR) dir..."
	cp -r $(SOURCE_DIR) $(DESTDIR)/usr

package:
	debuild --no-tgz-check --no-lintian --check-dirname-level 0

clean:
	echo
