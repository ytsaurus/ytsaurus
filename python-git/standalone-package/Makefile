#!/usr/bin/make -f

build:
	echo "Build"

install:
	echo "Using $(DESTDIR) dir..."
	mkdir -p "$(DESTDIR)/opt/$(PACKAGE)"
	for subdir in `find $(PYTHON_INSTALLDIR) -maxdepth 1 -not -path $(PYTHON_INSTALLDIR)`; do \
		cp -r "$$subdir" "$(DESTDIR)/opt/$(PACKAGE)" ; \
	done

package:
	debuild --no-tgz-check --no-lintian --check-dirname-level 0

clean:
	echo
