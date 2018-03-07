#!/usr/bin/perl
use strict;
use warnings;

my ($key, @kvp, %rec, %cache);

sub flush {
    my ($k, $v);

    while (($k, $v) = each %cache) {
        print "$k\thits=$v\n";
    }
    %cache = ();
}

while(<STDIN>) {
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
    $rec{vhost} or $rec{vhost} = 'no_vhost';
    $rec{ip} or $rec{ip} = $rec{'x_real_ip'};
    $rec{ip} or $rec{ip} = 'bad_ip' . rand() and $rec{vhost} = 'bad_ip ' . $rec{vhost};
    $key = $rec{ip};
    $cache{"_key=$key\tip=$rec{ip}\tvhost=$rec{vhost}"}++;
    if (scalar keys %cache > 100_000) {
        flush();
    }
}
flush();
