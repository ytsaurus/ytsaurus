#!/usr/local/bin/perl5 -w

use strict;
use Cwd;
use Sys::Hostname;

# cmake usually gets PWD variable that is not always too good
#delete $ENV{"PWD"};

my $wd = cwd;
my $call_cmake = 0;
my $build_dir_in_obj = 0;
my $verbose = 0;
my $call_unittest = 0;
my $alt_obj_search = hostname eq "mojito.yandex.ru";

if (defined $ARGV[0] && $ARGV[0] =~ /^verb/) {
  $verbose = 1;
  shift @ARGV;
}

if (defined $ARGV[0] && $ARGV[0] =~ /^cmake/) {
  $call_cmake = ($ARGV[0] !~ /_only/)? 1 : 2;
  $build_dir_in_obj = ($ARGV[0] !~ /_obj/)? 0 : 1;
  shift @ARGV;
}

if (defined $ARGV[0] && $ARGV[0] eq "obj") {
  $call_cmake = 1;
  $build_dir_in_obj = 1;
  shift @ARGV;
}

if (defined $ARGV[0] && $ARGV[0] eq "ut") {
  $call_unittest = 1;
  shift @ARGV;
}

# change this condition to make 'release' by default
if (!defined $ARGV[0] || $ARGV[0] ne "debug" && $ARGV[0] ne "release") {
  print STDERR "Usage: cmk.pl [verbose] [cmake[_only][_obj]] [debug|release] [arguments for make]\n".
    "   cmake      - call cmake for entire arcadia\n".
    "   cmake_only - call cmake for current project only\n".
    "   cmake_obj  - create cmake bin directory under /usr/obj\n".
    "   obj        - same as cmake_obj\n".
    "   ut         - unit-test\n".
    "   debug,release - targets, will become Debug and Release for cmake\n".
    "\n".
    "See also: https://wiki.yandex.ru/PoiskovajaPlatforma/BuildYandexUnixCmake/QuickStart#cmk\n";
  exit 1;
}

die "current dir $wd is in obj" if ($wd =~ /\/obj\//i);
die "current dir $wd is not in arcadia" if ($wd !~ /^(.*)\/(arcadia)($|\/.*$)/);

my ($prefix, $suffix, $target) = ($1, $3, $ARGV[0]);
$suffix =~ s!^/!!;
shift @ARGV;
$target = "release" if (!defined $target || $target eq "");

print " prefix=`$prefix'\n suffix=`$suffix'\n target=$target\n" if $verbose;

if ($call_cmake) {
  my $arcadia = "../arcadia";
  my $builddir = "$prefix/$target";
  if ($build_dir_in_obj) {
    $arcadia = "$prefix/arcadia";
    $builddir = "$prefix/arcadia/$target";
    my $objdir = "/usr/obj$prefix/arcadia-$target";
    system("mkdir -p $objdir") == 0
      or die ("mkdir -p failed");
    unlink $builddir if (-l $builddir);
    symlink($objdir, $builddir) or
      die ("could not create symlink: $!");
  } else {
    mkdir($builddir);
  }
  chdir($builddir) or die "$prefix/$target: $!";
  $target =~ s/^r/R/; $target =~ s/^d/D/;
  my $only = $call_cmake == 1? "" : "MAKE_ONLY=$suffix ";
  my $cmd = "${only}cmake -DCMAKE_BUILD_TYPE=$target $arcadia";
  print "===> ".cwd."\n$cmd\n";
  system($cmd) == 0 or
    die "call to `cmake' failed";
  exit 0;
}

my $builddir = "$prefix/$target";

if (! -d $builddir && -f "$prefix/arcadia/$target/CMakeCache.txt") {
  $builddir = "$prefix/arcadia/$target";
  $build_dir_in_obj = 1;
}

if (! -d $builddir) {
  print STDERR "Directory $prefix/$target does not exist\n".
    "You might want to call `cmk.pl cmake $target' first\n";
  die;
}

chdir("$builddir/$suffix") or die "$builddir/$suffix: $!";

foreach (@ARGV) {
  if (/\.o$/ && $alt_obj_search && !$build_dir_in_obj) {
    $_ = "$prefix/arcadia/$suffix/$_";
    $_ =~ s!^/!!;
    $_ =~ s!^.*/home/!home/!;
  }
}

if ($call_unittest) {
  system("pwd -P; ls -l") if $verbose;
  opendir(my $dh, ".");
  my @uts = grep { /_ut\.dir$/ && -d $_ } readdir($dh);
  closedir $dh;
  my $utfail = "";
  for my $ut (@uts) {
    print "-> $ut\n";
    chdir("$builddir/$suffix/$ut") or die "$builddir/$suffix/$ut: $!";
    $ut =~ s/\.dir$//;
    my $cmd = join " ", ("make $ut", @ARGV, "&& ./$ut");
    print "===> ".cwd."\n$cmd\n" if $verbose;
    system($cmd) == 0 or $utfail = "$utfail $ut";
  }
  die "Unit-tests failed: $utfail" if $utfail ne "";
  exit 0;
}

my $cmd = join " ", ("make", @ARGV);
print "===> ".cwd."\n$cmd\n" if $verbose;
system($cmd) == 0 or
  die "call to `make' failed";

1;
