#!/usr/bin/perl 
use strict;
use warnings;

my (%cache, @kvp, $key, %rec);
my %current;
my $prev = '';

sub flush {
    my ($k, $v);
    while (($k, $v) = each %cache) {
        print "_key=$k\t$k\tvisitors=$v->{visitors}\thits=$v->{hits}\n";
    }
    %cache = ();
}

sub write_current {
    my ($k, $v);
    while (($k, $v) = each %current) {
        defined $cache{$k} or $cache{$k} = { 
            hits=> 0, 
            visitors => 0,
        };
        $cache{$k}->{hits} += $v;
        $cache{$k}->{visitors} ++;
    }

    if (scalar keys %cache > 100_000) {
        flush();
    }
    %current = ();
}

while (<STDIN>) {
    chomp;
    ($key, @kvp) = split(/\t/, $_);
    %rec = map {
        if (defined $_) {
        my ($k, $v) = split('=', $_, 2);
            ($k => $v)
        } else {
            ();
        }
    } @kvp;
    
    if ($key ne $prev) {
        write_current();
        $prev = $key;
    }
    $current{"vhost=$rec{vhost}"} += $rec{hits};
}

write_current();
flush();
