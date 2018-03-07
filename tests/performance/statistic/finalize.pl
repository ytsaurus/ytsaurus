#!/usr/bin/perl 
use strict;
use warnings;

my (%cache, @kvp, $key, %rec);
my %current;
my $prev = '';

sub write_current {
    my ($k, $v);

    while (($k, $v) = each %current) {
        print "tskv\t$k\tvisitors=$v->{visitors}\thits=$v->{hits}\n";
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

    defined $current{"vhost=$rec{vhost}"} 
        or $current{"vhost=$rec{vhost}"} = {
        hits=> 0,
        visitors => 0,
    };
    $current{"vhost=$rec{vhost}"}->{visitors}+= $rec{visitors};
    $current{"vhost=$rec{vhost}"}->{hits} += $rec{hits};
}
write_current();
