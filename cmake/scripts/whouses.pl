#!/usr/bin/perl
use strict;
use Data::Dumper;

my ($dump, $warn);
$dump = 1 if grep /^-d$/, @ARGV;
$warn = 1 if grep /^-w$/, @ARGV;

sub getDeps($) {
    my $file = shift;

    my $uses = {};
# my $usedby = {};

    open DEPS, "< $file" or die "cannot open deps input: $file";
    while(<DEPS>) {
        m!^--! and next;
        m!^end$! and last;
        # m!<==! and next; # be strict
    
        m!^module: ((?:\w|\d|[/-])+) ?(LIB|PROG)?! or 
            die "not a unit definition:\n$_"; # next;
        my $unit = $1;
        my $utype = $2;
        $utype ||= "UNSET";
    
        print STDERR if($dump);
    
        my $depstr;
        while($depstr = <DEPS>) {
            print $depstr if($dump);

            ## maybe omit something
            # next if $unit eq "util/private/stl";
            # next if $unit =~ m!^util/!;
            # next if $unit =~ m!^contrib/!;
            # next if $unit =~ m!^junk/!;

            if ($depstr =~ m/^\s*}\s*$/) {
                last;
            }
            elsif ($depstr =~ m!^\s*peerdir: (\S*)!) {
                $depstr = $1;
                my @deps = split /;/, $depstr; 
                if($utype eq 'PROG') {
                    # set_vertex($unit, {color => "#E04040"});
                }

                if(exists $uses->{$unit} and $warn) {
                    print STDERR "already defined: $unit\n";
                }

                $uses->{$unit} = {};
                for my $rel (@deps) {
                    ## maybe omit something
                    next if $rel eq "util/private/stl";
                    # next if $rel =~ m!^util/!;
                    # next if $rel =~ m!^contrib/!;
                    if($dump) {
                        print STDERR " \"$unit\" -> \"$rel\";\n";
                    }
                    $uses->{$unit}{$rel} = 1;
                    # $usedby->{$rel}{$unit} = 1;
                }
            }
        }
    }
    return $uses;
}

sub saturate($) {
    my $uses = shift;
    my $updated = 0;
    do {
        $updated = 0;
        for my $unit(keys %$uses) {
            my $deps = $uses->{$unit};
            for my $dep (keys %$deps) {
                if(!exists $uses->{$dep}) {
                    print STDERR "no such unit: $dep required by $unit\n" if $warn;
                    next;
                }
                my $nldeps = $uses->{$dep};
                for my $nldep(keys %$nldeps) {
                    if(!exists $deps->{$nldep}) {
                        print STDERR "unsaturated: $unit => $nldep\n" if $dump;
                        $deps->{$nldep} = 1;
                        $updated++;
                    }
                }
            }
        }
        print STDERR "sat pass, ups = $updated\n" if $dump;
    } while($updated);

}

sub realDeps() {
    my $file = "cmake_deps";
    my $deps = getDeps($file);
    saturate($deps);
    print Dumper $deps if $dump;
    return $deps;
}

sub reverseDeps($) {
    my $hash = shift;
    my $res = {};
    for my $k (keys %$hash) {
        for my $v (keys %{$hash->{$k}}) {
            $res->{$v}{$k} = 1;
        }
    }
    return $res;
}

sub main() {
    my $uses = realDeps();
    my $usedby = reverseDeps($uses);

    print STDERR (scalar keys %$uses)." components\n" if $warn;
    print STDERR (scalar keys %$usedby)." rev components\n" if $warn;

    my $reverse;
    for my $unit (@ARGV) {
        if($unit eq '-r') {
            $reverse = 1;
            next;
        }
        die "no such unit: $unit" unless exists $uses->{$unit};
        unless($reverse) {
            print "$unit is used by\n";
            for my $user (sort keys %{$usedby->{$unit}}) {
                print "    $user\n";
            }
            if(not scalar keys %{$usedby->{$unit}}) {
                print "    void\n";
            }
        } 
        else {
            print "$unit uses\n";
            for my $dep (sort keys %{$uses->{$unit}}) {
                print "    $dep\n";
            }
        }
    }
}
main;
