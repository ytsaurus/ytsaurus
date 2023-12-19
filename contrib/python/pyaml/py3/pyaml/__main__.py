import os, sys, stat, tempfile, contextlib
import yaml, pyaml


@contextlib.contextmanager
def safe_replacement(path, *open_args, mode=None, xattrs=None, **open_kws):
	'Context to atomically create/replace file-path in-place unless errors are raised'
	path, xattrs = str(path), None
	if mode is None:
		try: mode = stat.S_IMODE(os.lstat(path).st_mode)
		except FileNotFoundError: pass
	if xattrs is None and getattr(os, 'getxattr', None): # MacOS
		try: xattrs = dict((k, os.getxattr(path, k)) for k in os.listxattr(path))
		except FileNotFoundError: pass
	open_kws.update( delete=False,
		dir=os.path.dirname(path), prefix=os.path.basename(path)+'.' )
	if not open_args: open_kws.setdefault('mode', 'w')
	with tempfile.NamedTemporaryFile(*open_args, **open_kws) as tmp:
		try:
			if mode is not None: os.fchmod(tmp.fileno(), mode)
			if xattrs:
				for k, v in xattrs.items(): os.setxattr(path, k, v)
			yield tmp
			if not tmp.closed: tmp.flush()
			try: os.fdatasync(tmp)
			except AttributeError: pass # MacOS
			os.rename(tmp.name, path)
		finally:
			try: os.unlink(tmp.name)
			except FileNotFoundError: pass


def main(argv=None):
	import argparse
	parser = argparse.ArgumentParser(
		description='Process and dump prettified YAML to stdout.')
	parser.add_argument('path', nargs='?', metavar='path',
		help='Path to YAML to read (default: use stdin).')
	parser.add_argument('-r', '--replace', action='store_true',
		help='Replace specified path with prettified version in-place.')
	parser.add_argument('-w', '--width', type=int, metavar='chars',
		help='Max line width hint to pass to pyyaml for the dump.'
			' Only used to format scalars and collections (e.g. lists).')
	opts = parser.parse_args(argv or sys.argv[1:])

	src = open(opts.path) if opts.path else sys.stdin
	try: data = yaml.safe_load(src)
	finally: src.close()

	pyaml_kwargs = dict()
	if opts.width: pyaml_kwargs['width'] = opts.width
	if opts.replace and opts.path:
		with safe_replacement(opts.path) as tmp:
			pyaml.pprint(data, file=tmp, **pyaml_kwargs)
	else: pyaml.pprint(data, **pyaml_kwargs)

if __name__ == '__main__': sys.exit(main())
